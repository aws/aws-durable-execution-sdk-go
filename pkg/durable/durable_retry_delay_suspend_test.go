package durable_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// TestStep_RetryDelaySuspendsThenResumes verifies the retry-delay
// suspension gap closed this session: a Step retry with a NON-ZERO delay
// suspends the whole invocation (returns PENDING) rather than
// re-executing fn immediately in-process, exactly like Wait suspends for
// its duration - see step.go's retryOrFail doc.
//
// Unlike TestWait_SuspendsThenResumes, fakeClient does NOT auto-resolve a
// RETRY action to a terminal status (only WAIT/START is special-cased -
// see fakeClient.Checkpoint's doc), so this test genuinely exercises
// suspension: the first invocation must observe PENDING with fn having
// run exactly once, and a second "replay" invocation (simulating the
// backend re-invoking once NextAttemptDelaySeconds elapses, with the
// retry operation now Pending in the operation log exactly as
// checkpointed) must re-run fn and reach a terminal status.
func TestStep_RetryDelaySuspendsThenResumes(t *testing.T) {
	client := newFakeClient()
	attempts := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		result, err := operations.Step(dc, "flaky", func(sc types.StepContext) (string, error) {
			attempts++
			if sc.Attempt() < 2 {
				return "", errors.New("transient failure")
			}
			return "ok", nil
		}, operations.WithStepRetryStrategy[string](utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: result}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "xyz"})

	// First invocation: fn fails once, the retry (30s delay) is
	// checkpointed, and the invocation should suspend rather than
	// re-execute fn immediately.
	firstInput := newInvocationInput("exec-retry-suspend-1", "arn:test:1", "token-0", eventPayload)
	out, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("unexpected error on first invocation: %v", err)
	}
	if out.Status != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING (suspended for retry delay), got status=%s error=%v", out.Status, out.Error)
	}
	if attempts != 1 {
		t.Fatalf("expected exactly 1 attempt before suspension, got %d", attempts)
	}

	snapshot := client.snapshot()
	stepOp, ok := snapshot[expectedStepID("exec-retry-suspend-1", 1)]
	if !ok {
		t.Fatal("expected step operation '1' (hashed) to be checkpointed")
	}
	if stepOp.Status != types.OperationStatusPending {
		t.Fatalf("expected checkpointed step status Pending after retry, got %s", stepOp.Status)
	}
	if stepOp.StepDetails == nil || stepOp.StepDetails.Attempt != 1 {
		t.Fatalf("expected checkpointed attempt count 1, got %+v", stepOp.StepDetails)
	}

	// Second invocation ("replay" after the backend's retry delay
	// elapses): seed InitialExecutionState with the checkpointed
	// operations from the fake backend, plus the root execution operation
	// with the same event payload, exactly as a real re-invocation would
	// receive them. runStep's OperationStatusPending branch then
	// re-executes fn directly (no further suspension - the backend
	// already decided it's time), and this time fn succeeds.
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-retry-suspend-1", "arn:test:1", "token-1", eventPayload, extraOps...)

	out, err = entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("unexpected error on second invocation: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded on second invocation, got status=%s error=%v", out.Status, out.Error)
	}
	if attempts != 2 {
		t.Fatalf("expected exactly 2 total attempts, got %d", attempts)
	}
}

// TestWaitForCondition_PollDelaySuspendsThenResumes verifies the same
// retry-delay suspension fix (see step.go's retryOrFail) applied to
// WaitForCondition's poll loop (wait_for_condition.go's pollAndCheckpoint):
// a poll retried with a NON-ZERO delay suspends the whole invocation
// rather than re-polling immediately in-process.
func TestWaitForCondition_PollDelaySuspendsThenResumes(t *testing.T) {
	client := newFakeClient()
	checks := 0

	handler := func(event orderEvent, dc types.DurableContext) (orderResult, error) {
		final, err := operations.WaitForCondition(dc, "poll-ready", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
			checks++
			state++
			return operations.ConditionResult[int]{State: state, ConditionMet: state >= 2}, nil
		}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 30}, 5)))
		if err != nil {
			return orderResult{}, err
		}
		return orderResult{OrderID: event.OrderID, Status: fmt.Sprintf("state=%d", final)}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client})
	eventPayload := mustMarshalPayload(t, orderEvent{OrderID: "xyz"})

	// First invocation: checkFn runs once (state=1, not yet met), the
	// retry (30s delay) is checkpointed, and the invocation should
	// suspend rather than poll again immediately.
	firstInput := newInvocationInput("exec-condition-suspend-1", "arn:test:1", "token-0", eventPayload)
	out, err := entry(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("unexpected error on first invocation: %v", err)
	}
	if out.Status != types.ExecutionStatusPending {
		t.Fatalf("expected PENDING (suspended for poll delay), got status=%s error=%v", out.Status, out.Error)
	}
	if checks != 1 {
		t.Fatalf("expected exactly 1 check before suspension, got %d", checks)
	}

	snapshot := client.snapshot()
	condOp, ok := snapshot[expectedStepID("exec-condition-suspend-1", 1)]
	if !ok {
		t.Fatal("expected condition operation '1' (hashed) to be checkpointed")
	}
	if condOp.Status != types.OperationStatusPending {
		t.Fatalf("expected checkpointed condition status Pending after retry, got %s", condOp.Status)
	}

	// Second invocation ("replay" after the backend's poll delay
	// elapses): checkFn runs again against the last-checkpointed state
	// (1) and this time reports ConditionMet.
	var extraOps []types.Operation
	for _, op := range snapshot {
		extraOps = append(extraOps, op)
	}
	secondInput := newInvocationInput("exec-condition-suspend-1", "arn:test:1", "token-1", eventPayload, extraOps...)

	out, err = entry(context.Background(), secondInput)
	if err != nil {
		t.Fatalf("unexpected error on second invocation: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		errMsg := "<nil>"
		if out.Error != nil {
			errMsg = out.Error.ErrorMessage
		}
		t.Fatalf("expected Succeeded on second invocation, got status=%s error=%s", out.Status, errMsg)
	}
	if checks != 2 {
		t.Fatalf("expected exactly 2 total checks, got %d", checks)
	}
}
