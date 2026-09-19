package main

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandlerReturnsResult calls the plain Lambda handler directly.
func TestHandlerReturnsResult(t *testing.T) {
	out, err := handler(context.Background(), map[string]any{"any": "event"})
	if err != nil {
		t.Fatal(err)
	}
	if out != "non_durable_result" {
		t.Errorf("result = %q, want %q", out, "non_durable_result")
	}
}

// TestHandlerAsInvokeTarget registers the handler as a plain (non-durable)
// target and invokes it from a durable caller.
func TestHandlerAsInvokeTarget(t *testing.T) {
	caller := func(ctx durable.Context, event any) (string, error) {
		return durable.Invoke[string](ctx, "plain", "plain-target", event)
	}
	runner := durabletest.NewLocalRunner(caller)
	runner.RegisterFunction("plain-target", durabletest.PlainFunction(handler))

	result := runner.RunUntilComplete(t, "event")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED; error = %+v", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatal(err)
	}
	if out != "non_durable_result" {
		t.Errorf("result = %q, want %q", out, "non_durable_result")
	}
}
