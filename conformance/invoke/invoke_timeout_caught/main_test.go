package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

const targetID = "target-function"

// runUntilInvokeOpen starts the handler and returns once the chained
// invoke is waiting for the target.
func runUntilInvokeOpen(t *testing.T) *durabletest.LocalRunner[any, string] {
	t.Helper()
	t.Setenv("TARGET_FUNCTION_NAME", targetID)
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING while the invoke is open", result.Status)
	}
	return runner
}

// TestHandlerReturnsFallbackOnTimeout times out the chained invoke and
// expects the execution to succeed with the fallback value.
func TestHandlerReturnsFallbackOnTimeout(t *testing.T) {
	runner := runUntilInvokeOpen(t)
	if err := runner.TimeoutChainedInvoke(""); err != nil {
		t.Fatalf("TimeoutChainedInvoke: %v", err)
	}

	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED; error = %+v", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatal(err)
	}
	if out != "fallback_result" {
		t.Errorf("result = %q, want %q", out, "fallback_result")
	}
	invokes := result.OperationsByType(string(durable.OperationTypeChainedInvoke))
	if len(invokes) != 1 || invokes[0].Status != string(durable.OperationStatusTimedOut) {
		t.Errorf("chained invokes = %+v, want one TIMED_OUT", invokes)
	}
}

// TestHandlerPropagatesOtherFailures fails the chained invoke with an
// ordinary error. Only the timeout is caught, so the execution fails.
func TestHandlerPropagatesOtherFailures(t *testing.T) {
	runner := runUntilInvokeOpen(t)
	if err := runner.FailChainedInvoke("", "TargetError", "target failed"); err != nil {
		t.Fatalf("FailChainedInvoke: %v", err)
	}

	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if result.Error == nil || result.Error.Type != "InvokeError" {
		t.Fatalf("error = %+v, want InvokeError", result.Error)
	}
}

// TestHandlerReturnsFallbackOnSuccess completes the chained invoke. The
// handler discards the target's result and returns the fallback value
// either way.
func TestHandlerReturnsFallbackOnSuccess(t *testing.T) {
	runner := runUntilInvokeOpen(t)
	if err := runner.CompleteChainedInvoke("", "target_result"); err != nil {
		t.Fatalf("CompleteChainedInvoke: %v", err)
	}

	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatal(err)
	}
	if out != "fallback_result" {
		t.Errorf("result = %q, want %q", out, "fallback_result")
	}
}
