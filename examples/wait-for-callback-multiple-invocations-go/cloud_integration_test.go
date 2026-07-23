//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to
// examples/wait-for-callback-multiple-invocations-go. Gated behind the
// cloudintegration build tag; makes REAL, billed, ASYNCHRONOUS Lambda
// Invoke(s) against a REAL deployed function in account 730758745077
// (region us-east-1), followed by real
// SendDurableExecutionCallbackSuccess calls from this test itself. Not
// wired into any CI workflow.
//
// This example has TWO sequential callbacks - RunUntilCallback/Continue
// is called twice, resolving each in turn, matching handler_test.go's
// own three-invocation LocalTestRunner pattern.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

const waitForCallbackMultipleInvocationsGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-callback-multiple-invocations-go-example:1"

func TestCloudTestRunner_Handler_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[MultiInvocationEvent, MultiInvocationResult](
		waitForCallbackMultipleInvocationsGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// Both real Wait operations (1 second each, real backend timing, not
	// SkipTime-accelerated) plus two real callback round-trips - a
	// generous Timeout accounts for the full real wall-clock cycle.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 3 * time.Minute

	event := MultiInvocationEvent{RequestID: "cloud-1"}

	// First callback.
	first, arn, err := runner.RunUntilCallback(ctx, "", event, "first-callback-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback (first): %v", err)
	}
	if first.GetStatus() != types.ExecutionStatusPending {
		msg, _ := first.GetError()
		t.Fatalf("expected PENDING after the first callback, got %s (%s)", first.GetStatus(), msg)
	}
	firstCallback, ok := first.GetOperationRecursive("first-callback-callback")
	if !ok {
		t.Fatal("expected to find 'first-callback-callback'")
	}
	if err := firstCallback.SendCallbackSuccess("first-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (first): %v", err)
	}

	// Resume by polling for the SECOND callback on the SAME execution:
	// RunUntilCallback accepts a non-empty arn to skip the (asynchronous)
	// Invoke call entirely and just poll an ALREADY-RUNNING execution for
	// the next named callback operation - exactly the capability needed
	// here, since Continue itself only ever polls to a genuine TERMINAL
	// status with no "stop at the next callback" mode (that mode is
	// RunUntilCallback's own, and it works on a resumed execution too via
	// this same non-empty-arn path).
	second, _, err := runner.RunUntilCallback(ctx, arn, event, "second-callback-callback")
	if err != nil {
		t.Fatalf("RunUntilCallback (second): %v", err)
	}
	if second.GetStatus() != types.ExecutionStatusPending {
		msg, _ := second.GetError()
		t.Fatalf("expected PENDING after the second callback, got %s (%s)", second.GetStatus(), msg)
	}
	secondCallback, ok := second.GetOperationRecursive("second-callback-callback")
	if !ok {
		t.Fatal("expected to find 'second-callback-callback'")
	}
	if err := secondCallback.SendCallbackSuccess("second-done"); err != nil {
		t.Fatalf("SendCallbackSuccess (second): %v", err)
	}

	final, err := runner.Continue(ctx, arn)
	if err != nil {
		t.Fatalf("Continue (final): %v", err)
	}
	if final.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := final.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", final.GetStatus(), msg)
	}
}
