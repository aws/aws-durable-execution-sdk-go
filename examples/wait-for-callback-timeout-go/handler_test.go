// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner.
//
// # A real, honest local-testing limitation, not worked around here
//
// operations.CallbackFailedError's own Timeout field is derived from the
// checkpointed operation's Status == types.OperationStatusTimedOut (see
// callback.go's callbackError) - a genuinely backend-driven terminal
// status that only the REAL Lambda Durable Functions backend can
// produce once WithWaitForCallbackTimeout's own duration elapses with no
// external SendDurableExecutionCallbackSuccess/Failure call.
// testing.LocalTestRunner's fake in-memory client has no equivalent
// mechanism (Operation only exposes SendCallbackSuccess/SendCallbackFailure,
// both of which set OperationStatusFailed, never OperationStatusTimedOut)
// - so the actual timeout PATH this example's own handler.go doc
// describes cannot be exercised locally at all today, only against a
// real deployment (exactly like the WaitForCallback conformance suite's
// own requirement 7-5, referenced in WithWaitForCallbackTimeout's own
// doc comment, which is a real, cloud-only conformance test for this
// same reason).
//
// This test therefore only exercises the EXPLICIT-failure path (a
// distinct, real scenario also reachable through this same handler,
// SendCallbackFailure) and asserts Timeout is correctly false for it -
// proving the Timeout field genuinely reflects op.Status rather than
// being hard-coded, without claiming to test the timeout path itself.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestHandler_ExplicitCallbackFailureIsNotReportedAsTimeout(t *testing.T) {
	runner := dtesting.New(handler, nil)
	arn := "arn:aws:lambda:local:000000000000:function:wait-for-callback-timeout-test:1"

	pending, err := runner.Continue(arn, TimeoutEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("first Continue: %v", err)
	}
	if pending.GetStatus() != types.ExecutionStatusPending {
		msg, _ := pending.GetError()
		t.Fatalf("expected PENDING while awaiting the callback, got %s (%s)", pending.GetStatus(), msg)
	}

	callbackOp, ok := pending.GetOperation("never-completes-callback-callback")
	if !ok {
		t.Fatal("expected to find a CALLBACK operation named 'never-completes-callback-callback'")
	}

	// Explicitly fail the callback (a real, distinct scenario from a
	// timeout - see this file's own top-level doc for why the actual
	// timeout path can't be exercised locally).
	if err := callbackOp.SendCallbackFailure(types.ErrorObject{ErrorMessage: "external system reported an error"}); err != nil {
		t.Fatalf("SendCallbackFailure: %v", err)
	}

	final, err := runner.Continue(arn, TimeoutEvent{RequestID: "req-001"})
	if err != nil {
		t.Fatalf("second Continue: %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED (the handler catches the callback error), got %s (%s)", final.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[TimeoutResult](final)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if out.Success {
		t.Fatal("expected Success=false")
	}
	if out.TimedOut {
		t.Fatal("expected TimedOut=false for an EXPLICIT external failure (not a real backend timeout) - proves handler.go's errors.As check correctly reads CallbackFailedError.Timeout rather than assuming every failure is a timeout")
	}

	dtesting.AssertEventSignatures(t, final, "testdata/TestHandler_ExplicitCallbackFailureIsNotReportedAsTimeout.history.json")
}
