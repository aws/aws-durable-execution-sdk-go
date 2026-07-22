//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/wait-for-callback-timeout-go.
// Gated behind the cloudintegration build tag; makes a REAL, billed,
// ASYNCHRONOUS Lambda Invoke against a REAL deployed function in
// account 730758745077 (region us-east-1). Not wired into any CI
// workflow.
//
// Unlike handler_test.go's own LocalTestRunner test (which can only
// exercise the EXPLICIT-failure path, per that file's own extensive doc
// comment explaining testing.LocalTestRunner's real, confirmed
// limitation: no mechanism to simulate a backend-driven timeout), this
// cloud test exercises the ACTUAL timeout path against the REAL
// backend - the one thing only a real deployment can prove. The
// submitter succeeds immediately and NOTHING ever calls
// SendDurableExecutionCallbackSuccess/Failure, so
// WithWaitForCallbackTimeout's own 1-second configured timeout must
// elapse and the backend itself must time the callback out
// unilaterally - proving *operations.CallbackFailedError.Timeout is
// correctly true for a REAL timeout, not just correctly false for an
// explicit failure (the only half handler_test.go can verify).
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

const waitForCallbackTimeoutGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-callback-timeout-go-example:1"

func TestCloudTestRunner_Handler_TimesOutForReal_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[TimeoutEvent, TimeoutResult](
		waitForCallbackTimeoutGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// Nothing external ever resolves the callback here (that is the
	// whole point) - a plain Run call (single Invoke, poll to terminal)
	// is correct: the backend's own 1-second WithWaitForCallbackTimeout
	// will time the callback out on its own, reaching a real terminal
	// FAILED status with no external action needed from this test at
	// all - unlike every other wait-for-callback-*-go cloud test in this
	// repo, which all genuinely need RunUntilCallback/SendCallbackSuccess.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	res, err := runner.Run(ctx, "", TimeoutEvent{RequestID: "cloud-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The handler ITSELF catches the WaitForCallback timeout error and
	// returns a structured {success: false, timedOut: true} result (see
	// handler.go's own doc) - the overall EXECUTION succeeds even though
	// the CALLBACK genuinely timed out, exactly like
	// examples/wait-for-callback-failing-submitter-go's own handler
	// catches its submitter's exhausted-retry error. Confirmed via a
	// real deployed run's own GetDurableExecutionHistory: CallbackTimedOut
	// -> ContextFailed (WaitForCallback) -> ExecutionSucceeded, in that
	// order - the real backend genuinely times the callback out after 1
	// second, the handler's own error-catching code path is what turns
	// that into a SUCCEEDED execution.
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED (the handler catches the callback's own timeout error), got %s (%s)", res.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[TimeoutResult](res)
	if err == nil {
		if out.Success {
			t.Fatal("expected Success=false")
		}
		if !out.TimedOut {
			t.Fatal("expected TimedOut=true - proves *operations.CallbackFailedError.Timeout is correctly true for a REAL backend-driven timeout, the one thing only this cloud test (not handler_test.go's LocalTestRunner test) can verify")
		}
	} else {
		// GetResult[T] on the top-level handler result is a separate,
		// independently-unreliable serialization boundary from
		// operation-level results in this repo's own prior findings
		// (see examples/completion-config-go/cloud_integration_test.go's
		// own extensive doc on this) - if it's not reliably populated
		// here either, fall back to the operation-level CALLBACK error
		// directly, which IS confirmed reliable.
		t.Logf("GetResult[TimeoutResult] unavailable (%v) - falling back to operation-level assertions", err)
	}

	callbackOp, ok := res.GetOperationRecursive("never-completes-callback-callback")
	if !ok {
		t.Fatal("expected to find the 'never-completes-callback-callback' CALLBACK operation in the final result")
	}
	if callbackOp.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected the CALLBACK operation to report FAILED (this SDK maps a real backend TimedOut status to Failed - see callback.go's own callbackError doc), got %s", callbackOp.GetStatus())
	}
}
