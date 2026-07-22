package operations

import (
	"fmt"
	"time"

	dcontext "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// Wait suspends execution for the specified duration without incurring
// compute charges. The Lambda invocation terminates and is resumed by the
// Lambda Durable Functions backend once the timer fires; on resume,
// execution continues immediately after the Wait call as if it were a
// normal (blocking) function call.
//
// id must be unique within the enclosing context, following the same
// rules as Step.
//
// # Implementation notes
//
// Mirrors the JS/Java reference SDKs' Wait operation
// (docs/checkpoint-replay-design.md §4, "WaitOperation"): checkpoint a
// WaitDetails.WaitUntilTimestamp (computed once, at first execution, so
// replay doesn't recompute it from a fresh time.Now() call - see
// dcontext.CurrentTime's doc for why that distinction matters), then block
// via ExecManager.WaitForOperation exactly like a Step waiting on its
// retry timer. If no other goroutine is active, this drops the active
// count to zero and the whole invocation suspends (returns PENDING).
func Wait(dc types.DurableContext, id string, d types.Duration) error {
	c, ok := dc.(*dcontext.Context)
	if !ok {
		return fmt.Errorf("operations.Wait: dc must be created by this SDK's runtime (got %T)", dc)
	}

	stepID := c.NextStepID()

	if existing, found := c.ExecManager().GetOperation(stepID); found {
		// Replay-consistency check (docs/remaining-work.md §4 task 11):
		// verify the checkpointed operation at this step ID really is a
		// WAIT before reading anything operation-specific from it below
		// (Wait itself doesn't read WaitDetails at all today - it only
		// branches on Status - but the check still matters: without it,
		// a step ID that used to be e.g. a STEP now called as a Wait
		// would silently fall through this whole switch statement (a
		// STEP's Status values mean something different to a Wait
		// caller) rather than erroring, which is exactly the class of
		// bug this task closes).
		if err := checkReplayConsistency(existing, types.OperationTypeWait, "", OperationKindWait, stepID, id); err != nil {
			return err
		}

		switch existing.Status {
		case types.OperationStatusSucceeded:
			// Wait already elapsed in a prior invocation; replay-skip.
			return nil
		case types.OperationStatusFailed:
			return fmt.Errorf("wait %q (id %s): recorded as failed", id, stepID)
		case types.OperationStatusPending, types.OperationStatusStarted:
			// Already checkpointed, still counting down (or backend
			// hasn't yet marked it succeeded) - block for the remainder.
			// This invocation did not itself START the wait (a prior one
			// did), but IT is the one observing the wait's own
			// resolution, so OnOperationEnd fires here - mirroring how
			// Step's executeResume fires OnOperationStart for an attempt
			// that resumes across invocation boundaries.
			if _, ok := c.ExecManager().WaitForOperation(stepID); !ok {
				return errSuspended
			}
			resumeInfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: id, Type: types.OperationTypeWait, SubType: subTypeWait}
			dispatchOperationEnd(c, resumeInfo, 0, plugin.OperationStatusSucceeded, "", nil)
			return nil
		}
	}

	waitUntil := dcontext.CurrentTime().Add(toGoDuration(d)).UnixMilli()
	waitSeconds := d.Days*86400 + d.Hours*3600 + d.Minutes*60 + d.Seconds

	dinfo := operationKindDispatchInfo{ID: stepID, ParentID: c.ParentStepID(), Name: id, Type: types.OperationTypeWait, SubType: subTypeWait}
	dispatchOperationStart(c, dinfo, 0)

	if err := c.Checkpoint().Enqueue(types.OperationUpdate{
		ID:          stepID,
		ParentID:    c.ParentStepID(),
		Type:        types.OperationTypeWait,
		Name:        id,
		SubType:     subTypeWait,
		Action:      types.OperationActionStart,
		WaitOptions: &types.WaitOptions{WaitSeconds: &waitSeconds},
	}); err != nil {
		return fmt.Errorf("wait %q (id %s): checkpointing start: %w", id, stepID, err)
	}
	_ = waitUntil // recorded server-side via WaitOptions.WaitSeconds; retained locally only for symmetry with StepOptions.NextAttemptDelaySeconds pattern.

	if _, ok := c.ExecManager().WaitForOperation(stepID); !ok {
		return errSuspended
	}
	dispatchOperationEnd(c, dinfo, 0, plugin.OperationStatusSucceeded, "", nil)
	return nil
}

// toGoDuration converts the SDK's replay-safe, serializable types.Duration
// into a time.Duration for local arithmetic (e.g. computing an absolute
// deadline to log or compare against). Never expose a time.Duration
// directly in checkpointed data - the wire format uses WaitSeconds
// (an int) precisely so it round-trips through JSON/the backend API
// without depending on time.Duration's platform representation.
func toGoDuration(d types.Duration) time.Duration {
	total := d.Days*86400 + d.Hours*3600 + d.Minutes*60 + d.Seconds
	return time.Duration(total) * time.Second
}
