package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// TestNonDeterministicReplay_StepThenWaitAtSameID is the task-11 "real
// non-deterministic-replay scenario" test: checkpoint a STEP at step ID
// "1" on a first invocation, then - simulating a deployment that changed
// the handler's code so the very first durable operation is now a Wait
// instead of a Step - replay with a SECOND invocation of a DIFFERENT
// handler closure that calls operations.Wait at that same step ID.
//
// Before docs/remaining-work.md §4 task 11's replay-consistency check
// existed, this would have silently misbehaved exactly as the task
// describes: operations.Wait's runStep-equivalent switch only branches on
// existing.Status (Succeeded/Failed/Pending/Started), and a checkpointed
// STEP's Status of Succeeded looks identical to a checkpointed WAIT's
// Status of Succeeded - so Wait would have replay-skipped and returned
// nil (success) without ever noticing the checkpointed operation was
// never actually a WAIT at all, silently returning a "successful wait"
// that never really waited on anything.
//
// With the fix, this must instead surface a clearly-typed
// *operations.NonDeterministicReplayError explaining the mismatch.
func TestNonDeterministicReplay_StepThenWaitAtSameID(t *testing.T) {
	client := newFakeClient()
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "nondeterministic-1"})

	// First invocation: handler's current (later-to-be-"old") code calls
	// a Step at the very first step ID ("1").
	stepHandler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Step(dc, "was-a-step", func(sc types.StepContext) (string, error) {
			return "step-result", nil
		})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	entry1 := durable.WithDurableExecution(stepHandler, &durable.Config{Client: client})
	firstInput := newInvocationInput("exec-nondeterministic-1", "arn:test:nondeterministic-1", "token-0", eventPayload)

	out1, err := entry1(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("first invocation transport error: %v", err)
	}
	if out1.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected first invocation Succeeded, got status=%s error=%v", out1.Status, out1.Error)
	}

	// Confirm the checkpoint really did record a STEP at ID "1" - this is
	// the checkpointed state a real backend would hand back on the next
	// invocation of a REDEPLOYED handler.
	snapshot := client.snapshot()
	checkpointedStep, ok := snapshot[expectedStepID("exec-nondeterministic-1", 1)]
	if !ok {
		t.Fatal("expected step '1' (hashed) to be checkpointed")
	}
	if checkpointedStep.Type != types.OperationTypeStep {
		t.Fatalf("expected checkpointed operation '1' to be Type=STEP, got %s", checkpointedStep.Type)
	}

	// Second invocation ("replay" against a DIFFERENT handler, simulating
	// a deployment where the handler's code changed so the first durable
	// operation is now a Wait instead of a Step at the same step ID):
	// seed InitialExecutionState with the checkpointed STEP operation
	// exactly as a real re-invocation would receive it.
	waitHandler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		err := operations.Wait(dc, "now-a-wait", types.Duration{Seconds: 5})
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: "waited"}, nil
	}

	entry2 := durable.WithDurableExecution(waitHandler, &durable.Config{Client: client})
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-nondeterministic-1", "arn:test:nondeterministic-1", "token-1", eventPayload, extraOps...)

	out2, err := entry2(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("second invocation transport error: %v", err)
	}

	// The critical assertion: this must be a clearly-typed failure, NOT
	// a silent "success" from Wait replay-skipping a STEP's Succeeded
	// status as if it were its own.
	if out2.Status != types.ExecutionStatusFailed {
		t.Fatalf("expected the replay-inconsistent Wait call to FAIL, got status=%s (result=%v) - this means the non-deterministic replay was silently swallowed instead of detected", out2.Status, out2.ResultPayload)
	}
	if out2.Error == nil || out2.Error.ErrorMessage == "" {
		t.Fatal("expected a non-empty error message describing the non-determinism")
	}

	// The error message (all durable.WithDurableExecution can surface,
	// nested under out2.Error.ErrorMessage - see durable.go's
	// outcomeToResponse and types.DurableExecutionOutput's own doc for
	// why this is a nested types.ErrorObject, not a flat field)
	// must be traceable back to operations.NonDeterministicReplayError.
	// Re-derive the structured error directly from the operations
	// package's own check to confirm the message text this test just
	// observed really does come from that type, rather than merely
	// asserting on a substring of the flattened message (which could
	// pass for the wrong reason).
	mismatchErr := checkReplayConsistencyForTest(checkpointedStep, types.OperationTypeWait, "now-a-wait")
	var ndErr *operations.NonDeterministicReplayError
	if !errors.As(mismatchErr, &ndErr) {
		t.Fatalf("expected *operations.NonDeterministicReplayError from the same checkpointed state, got %T: %v", mismatchErr, mismatchErr)
	}
	if out2.Error.ErrorMessage != ndErr.Error() {
		t.Fatalf("expected the invocation's ErrorMessage to match NonDeterministicReplayError.Error() exactly\n  got:      %q\n  expected: %q", out2.Error.ErrorMessage, ndErr.Error())
	}
}

// checkReplayConsistencyForTest re-derives what operations.Wait's own
// internal replay-consistency check would have produced for the given
// checkpointed operation, so this test can assert the SAME error type
// and message the actual call path produced, without needing
// checkReplayConsistency itself (an unexported function) to be
// accessible from this external test package. Mirrors exactly what
// wait.go's Wait does: expects OperationTypeWait, no SubType.
func checkReplayConsistencyForTest(existing types.Operation, expectedType types.OperationType, name string) error {
	if existing.Type == expectedType {
		return nil
	}
	return &operations.NonDeterministicReplayError{
		OperationError: &operations.OperationError{ID: existing.ID, Name: name},
		ExpectedType:   expectedType,
		ActualType:     existing.Type,
		ActualSubType:  existing.SubType,
	}
}
