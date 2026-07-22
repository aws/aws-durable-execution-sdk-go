// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner.
//
// This Go SDK has no SendDurableExecutionCallbackHeartbeat-equivalent
// test helper exposed on testing.Operation (only SendCallbackSuccess/
// SendCallbackFailure) - so, like
// examples/wait-for-callback-timeout-go's own honest disclosure, the
// actual heartbeat-KEEPS-IT-ALIVE mechanism itself cannot be exercised
// locally. Nor does testing.Operation expose the checkpointed
// CallbackOptions.HeartbeatTimeoutSeconds value back for inspection
// (types.Operation carries no CallbackOptions field at all on the
// read side - only StepDetails/WaitDetails/CallbackDetails/etc, none of
// which retain the original request-side options) - so this test
// cannot independently confirm the exact HeartbeatTimeoutSeconds value
// either, only that the option compiles and the operation otherwise
// behaves like any other successful callback. This test therefore only
// confirms the callback completes normally via SendCallbackSuccess,
// exactly like examples/wait-for-callback-go's own basic test - it is a
// real, narrow gap in this Go SDK's own testing package (not something
// this example works around) that CallbackOptions round-tripping isn't
// independently verifiable through LocalTestRunner today.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_CompletesAfterHeartbeatConfiguredCallback(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:wait-for-callback-heartbeat-test:1"

	pending, err := runner.Continue(arn, HeartbeatEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING while awaiting the callback, got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperation("long-running-task-callback-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'long-running-task-callback-callback'")
	}
	if callbackOp.GetStatus() != types.OperationStatusStarted {
		t.Fatalf("expected callback STARTED, got %s", callbackOp.GetStatus())
	}

	if err := callbackOp.SendCallbackSuccess("done"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}

	final, err := runner.Continue(arn, HeartbeatEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[HeartbeatResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if !out.Completed {
		t.Fatal("expected Completed=true")
	}

	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_CompletesAfterHeartbeatConfiguredCallback.history.json")
}
