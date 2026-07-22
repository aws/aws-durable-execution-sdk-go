// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner across TWO invocations: SkipTime fast-forwards the
// submitter's own retry delays (so the submitter step itself reaches a
// terminal SUCCEEDED status within the first invocation), but
// WaitForCallback's own outer callback ALWAYS requires an explicit
// external SendCallbackSuccess/Failure - exactly like
// examples/wait-for-callback-go's own two-invocation pattern - regardless
// of how quickly (or without any failures at all) the submitter itself
// completes.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_SucceedsAfterTransientFailures(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:retry-success-test:1"

	// Fails on attempts 1 and 2, succeeds on attempt 3 - all resolved
	// synchronously within this first invocation via SkipTime.
	pending, err := runner.Continue(arn, RetrySuccessEvent{RequestID: "req-001", FailUntilAttempt: 3})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING (submitter recovered, but the callback itself is still outstanding), got %s (%s)", pending.GetStatus(), msg)
	}

	submitStep, ok := pending.GetOperation("retry-submitter-callback-submit")
	if !ok {
		t.Fatal("expected to find the submitter STEP operation 'retry-submitter-callback-submit'")
	}
	if submitStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the submitter step to end SUCCEEDED after recovering, got %s", submitStep.GetStatus())
	}

	callbackOp, ok := pending.GetOperation("retry-submitter-callback-callback")
	if !ok {
		t.Fatal("expected to find the CALLBACK operation 'retry-submitter-callback-callback'")
	}
	if err := callbackOp.SendCallbackSuccess("resolved"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(arn, RetrySuccessEvent{RequestID: "req-001", FailUntilAttempt: 3})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[RetrySuccessResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if !out.Success {
		t.Fatal("expected Success=true")
	}
	if out.Result != "resolved" {
		t.Fatalf("expected Result 'resolved', got %q", out.Result)
	}

	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_SucceedsAfterTransientFailures.history.json")
}

func TestHandler_SucceedsImmediatelyWithNoFailures(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:retry-success-immediate-test:1"

	pending, err := runner.Continue(arn, RetrySuccessEvent{RequestID: "req-002", FailUntilAttempt: 1})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING, got %s (%s)", pending.GetStatus(), msg)
	}

	submitStep, ok := pending.GetOperation("retry-submitter-callback-submit")
	if !ok {
		t.Fatal("expected to find the submitter STEP operation")
	}
	if submitStep.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the submitter step to end SUCCEEDED, got %s", submitStep.GetStatus())
	}

	callbackOp, ok := pending.GetOperation("retry-submitter-callback-callback")
	if !ok {
		t.Fatal("expected to find the CALLBACK operation")
	}
	if err := callbackOp.SendCallbackSuccess("resolved"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(arn, RetrySuccessEvent{RequestID: "req-002", FailUntilAttempt: 1})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}
}
