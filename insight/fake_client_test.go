package insight

import (
	"context"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// fakeClient is a minimal in-memory checkpoint.Client test double for
// exercising the Workflow Insight plugin against a real
// durable.WithDurableExecution run, without needing AWS credentials or a
// deployment. This is a small, insight-module-local copy of the same
// kind of test double the root module's own durable_test.go family
// uses internally (that one is unexported and untestable from outside
// the root module, so this package - a separate Go module, per this
// package's own doc on why - needs its own).
//
// Applies each OperationUpdate directly (no retry-delay simulation, no
// SkipTime concept - this module's own tests only need synchronous,
// single-attempt operations to exercise the plugin's record-building
// logic, not the full checkpoint/replay state machine already covered
// exhaustively by the root module's own test suite).
type fakeClient struct {
	mu  sync.Mutex
	ops map[string]types.Operation
}

func newFakeClient() *fakeClient {
	return &fakeClient{ops: make(map[string]types.Operation)}
}

func (f *fakeClient) Checkpoint(_ context.Context, req types.CheckpointDurableExecutionRequest) (*types.CheckpointDurableExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var updated []types.Operation
	for _, u := range req.Updates {
		op := f.ops[u.ID]
		op.ID = u.ID
		op.ParentID = u.ParentID
		op.Name = u.Name
		op.Type = u.Type
		if u.SubType != "" {
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
				if op.ContextDetails == nil {
					op.ContextDetails = &types.ContextDetails{}
				}
				op.ContextDetails.Result = u.Payload
			}
			if u.Type == types.OperationTypeWait {
				// no result payload for Wait
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
		}

		if u.Type == types.OperationTypeWait && u.Action == types.OperationActionStart {
			// Simulate the wait completing immediately, matching the
			// root module's own fakeClient precedent (durable_fake_client_test.go).
			op.Status = types.OperationStatusSucceeded
		}

		f.ops[u.ID] = op
		updated = append(updated, op)
	}

	return &types.CheckpointDurableExecutionResponse{
		UpdatedOperations: updated,
	}, nil
}
