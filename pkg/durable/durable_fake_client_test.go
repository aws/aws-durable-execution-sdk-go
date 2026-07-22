package durable_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// fakeClient is a minimal in-memory implementation of checkpoint.Client
// for exercising the checkpoint/replay state machine without a real
// backend. It applies each OperationUpdate to an in-memory operation map
// exactly as a real backend would (START marks Started, SUCCEED/FAIL mark
// terminal, RETRY marks Pending and persists any checkpointed Payload -
// see the OperationActionRetry case's doc for why that matters), and
// echoes back the updated operations so execmgr.Manager's in-memory cache
// stays consistent - mirroring what the real CheckpointDurableExecution
// API response would provide. Field names/shapes match the real API as
// verified via `aws lambda checkpoint-durable-execution help`.
type fakeClient struct {
	mu    sync.Mutex
	ops   map[string]types.Operation
	token int
}

func newFakeClient() *fakeClient {
	return &fakeClient{ops: make(map[string]types.Operation)}
}

func (f *fakeClient) Checkpoint(_ context.Context, req types.CheckpointDurableExecutionRequest) (*types.CheckpointDurableExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Real-backend constraint, confirmed via a genuine live deployment
	// failure (examples/map-with-condition-and-callback-go invoked
	// against a real Lambda function, account 730758745077, us-east-1;
	// see wait_for_condition.go's package-level bug writeup for the full
	// investigation): a RETRY action's OperationUpdate must not carry
	// BOTH Error and Payload. The real backend rejects this outright
	// with "InvalidParameterValueException: Cannot provide both an Error
	// and Payload for RETRY action." This fake client (a hand-rolled
	// checkpoint.Client test double predating the real backend
	// integration) previously applied ANY combination of fields
	// unconditionally, which is exactly why this specific bug shipped to
	// a real deployment without any local test catching it first -
	// closing that gap is as important as the operations.go fix itself
	// (see this task's own instructions). Checked up front, against every
	// update in the batch, before any of them are applied - a real
	// backend validates and rejects the WHOLE checkpoint request
	// atomically, it does not partially apply a batch and then fail
	// partway through.
	for _, u := range req.Updates {
		if u.Action == types.OperationActionRetry && u.Error != nil && u.Payload != nil {
			return nil, fmt.Errorf("checkpoint API error (NON_RETRYABLE): InvalidParameterValueException: Cannot provide both an Error and Payload for RETRY action (operation id %s)", u.ID)
		}
	}

	var updated []types.Operation
	for _, u := range req.Updates {
		op := f.ops[u.ID]
		op.ID = u.ID
		op.ParentID = u.ParentID
		op.Name = u.Name
		op.Type = u.Type
		if u.SubType != "" {
			// Persist SubType exactly like inmemory_client.go's
			// in-memory test client already does (see that file's
			// comment at the equivalent line) - found missing here
			// while implementing docs/remaining-work.md §4 task 11's
			// replay-consistency check: WaitForCondition checkpoints
			// STEP/WAIT_FOR_CONDITION, and without persisting SubType
			// here, every operation this fakeClient stores for a
			// WaitForCondition call came back with SubType=="" on the
			// FOLLOWING invocation's GetOperation lookup - which the
			// new checkReplayConsistency check (correctly) flagged as
			// "looks like a plain Step now, not a WaitForCondition,"
			// even though nothing about the actual handler code had
			// changed. A real backend would of course persist whatever
			// SubType was checkpointed, exactly as it does for every
			// other field on the request - this fakeClient (a
			// hand-rolled test double predating the real backend
			// integration) had simply never round-tripped it, and
			// nothing before this task ever read SubType back out on
			// the replay path to notice the gap.
			op.SubType = u.SubType
		}

		switch u.Action {
		case types.OperationActionStart:
			op.Status = types.OperationStatusStarted
		case types.OperationActionSucceed:
			op.Status = types.OperationStatusSucceeded
			if u.Type == types.OperationTypeStep {
				if op.StepDetails == nil {
					op.StepDetails = &types.StepDetails{}
				}
				op.StepDetails.Result = u.Payload
			}
			if u.Type == types.OperationTypeContext {
				// Round-trip Result AND ContextOptions.ReplayChildren
				// into ContextDetails, exactly like Step's own
				// StepDetails.Result handling above - found missing
				// here while adding operations.RunInChildContext's new
				// ReplayChildren protocol (see invoke.go's own doc):
				// this fakeClient (a hand-rolled test double predating
				// that feature) had never round-tripped ANY
				// ContextDetails field at all, for either Result or
				// ReplayChildren, which is exactly why no local test
				// could previously observe either one on the replay
				// path - a real backend persists whatever was
				// checkpointed, exactly as it already does for every
				// other operation type/field this fakeClient handles.
				if op.ContextDetails == nil {
					op.ContextDetails = &types.ContextDetails{}
				}
				op.ContextDetails.Result = u.Payload
				if u.ContextOptions != nil {
					op.ContextDetails.ReplayChildren = u.ContextOptions.ReplayChildren
				}
			}
		case types.OperationActionFail:
			op.Status = types.OperationStatusFailed
			if u.Type == types.OperationTypeStep {
				if op.StepDetails == nil {
					op.StepDetails = &types.StepDetails{}
				}
				op.StepDetails.Error = u.Error
			}
			if u.Type == types.OperationTypeContext {
				if op.ContextDetails == nil {
					op.ContextDetails = &types.ContextDetails{}
				}
				op.ContextDetails.Error = u.Error
			}
		case types.OperationActionRetry:
			op.Status = types.OperationStatusPending
			if u.Type == types.OperationTypeStep {
				if op.StepDetails == nil {
					op.StepDetails = &types.StepDetails{}
				}
				op.StepDetails.Attempt++
				op.StepDetails.Error = u.Error
				// A RETRY checkpoint's Payload carries the latest
				// checkpointed state (WaitForCondition's poll state -
				// see wait_for_condition.go's pollAndCheckpoint; plain
				// Step retries never set Payload, so this is a no-op
				// for those). A real backend persists whatever was sent
				// in the checkpoint request, and
				// runWaitForCondition's OperationStatusPending branch
				// reads it back via deserializeConditionState on the
				// next invocation - so this must round-trip here too,
				// exactly like SUCCEED already does above.
				if u.Payload != nil {
					op.StepDetails.Result = u.Payload
				}
			}
		case types.OperationActionCancel:
			op.Status = types.OperationStatusCancelled
		}

		if u.Type == types.OperationTypeWait && u.Action == types.OperationActionStart {
			// Simulate the wait completing immediately for test purposes
			// (a real backend would only mark it Succeeded once the
			// duration elapses; tests that need to observe the
			// PENDING/suspended state do so before this synchronous
			// Checkpoint call returns, by construction of the test).
			op.Status = types.OperationStatusSucceeded
		}

		f.ops[u.ID] = op
		updated = append(updated, op)
	}

	f.token++
	nextToken := fmt.Sprintf("token-%d", f.token)
	return &types.CheckpointDurableExecutionResponse{
		NextCheckpointToken: &nextToken,
		UpdatedOperations:   updated,
	}, nil
}

// snapshot returns a copy of all recorded operations, for assertions.
func (f *fakeClient) snapshot() map[string]types.Operation {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]types.Operation, len(f.ops))
	for k, v := range f.ops {
		out[k] = v
	}
	return out
}
