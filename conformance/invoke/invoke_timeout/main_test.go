package main

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

const targetID = "target-function"

// TestHandlerPropagatesTimeout times out the chained invoke and expects
// the execution to fail with the invoke error, as the reference does when
// the target runs past its timeout.
func TestHandlerPropagatesTimeout(t *testing.T) {
	t.Setenv("TARGET_FUNCTION_NAME", targetID)
	runner := durabletest.NewLocalRunner(handler)

	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING while the invoke is open", result.Status)
	}
	if err := runner.TimeoutChainedInvoke(""); err != nil {
		t.Fatalf("TimeoutChainedInvoke: %v", err)
	}

	result = runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
	if result.Error == nil || result.Error.Type != "InvokeError" {
		t.Fatalf("error = %+v, want InvokeError", result.Error)
	}
	invokes := result.OperationsByType(string(durable.OperationTypeChainedInvoke))
	if len(invokes) != 1 {
		t.Fatalf("got %d chained invokes, want 1", len(invokes))
	}
	if invokes[0].Status != string(durable.OperationStatusTimedOut) {
		t.Errorf("invoke status = %s, want TIMED_OUT", invokes[0].Status)
	}
}

// TestHandlerInvokesTargetFromEnvironment registers a target under the
// identifier named by TARGET_FUNCTION_NAME and expects the handler to
// return that target's result.
func TestHandlerInvokesTargetFromEnvironment(t *testing.T) {
	t.Setenv("TARGET_FUNCTION_NAME", targetID)
	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction(targetID, durabletest.PlainFunction(func(_ context.Context, _ any) (string, error) {
		return "target_result", nil
	}))

	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatal(err)
	}
	if out != "target_result" {
		t.Errorf("result = %q, want %q", out, "target_result")
	}
}
