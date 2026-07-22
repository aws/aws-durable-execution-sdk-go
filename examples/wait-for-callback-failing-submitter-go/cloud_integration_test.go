//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to
// examples/wait-for-callback-failing-submitter-go. Gated behind the
// cloudintegration build tag; makes a REAL, billed, synchronous Lambda
// Invoke against a REAL deployed function in account 730758745077
// (region us-east-1). Not wired into any CI workflow.
//
// This example's submitter ALWAYS fails, exhausting its own retry
// policy - the whole operation fails on the submitter's own STEP
// failure, with NO suspension ever required (CreateCallback registers a
// real CallbackID with the backend before the submitter runs, per
// callback.go's own implementation order, but nothing external ever
// needs to resolve it - the submitter's own exhausted retries fail the
// whole WaitForCallback operation first) - so this stays a single,
// synchronous Run call, unlike examples/create-callback-go's own
// RunUntilCallback-based cloud test.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// waitForCallbackFailingSubmitterGoExampleFunctionARN is the real,
// deployed function's qualified version-1 ARN, confirmed live via a
// real smoke-test Invoke immediately before writing this test.
const waitForCallbackFailingSubmitterGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-for-callback-failing-submitter-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[SubmitterAttemptEvent, SubmitterAttemptResult](
		waitForCallbackFailingSubmitterGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	// The submitter's own 1-second retry delay (3 attempts) means this
	// real invocation genuinely takes several real seconds against the
	// backend's own retry-delay suspend/resume mechanism internally
	// (unlike LocalTestRunner's SkipTime, the real backend does not
	// fast-forward) - a longer Timeout accounts for this.
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	res, err := runner.Run(ctx, "", SubmitterAttemptEvent{RequestID: "cloud-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The handler itself catches the WaitForCallback error and returns a
	// structured result - the overall execution SUCCEEDS even though the
	// submitter never did (see handler.go's own doc).
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED (the handler catches the submitter's exhausted-retry error), got %s (%s)", res.GetStatus(), msg)
	}

	submitStep, ok := res.GetOperationRecursive("failing-submitter-callback-submit")
	if !ok {
		t.Fatal("expected to find the submitter STEP operation 'failing-submitter-callback-submit' in the real, cloud-polled operation log")
	}
	if submitStep.GetStatus() != types.OperationStatusFailed {
		t.Fatalf("expected the submitter step to end FAILED after exhausting retries, got %s", submitStep.GetStatus())
	}
}
