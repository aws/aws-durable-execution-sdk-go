//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring (see examples/simple-step-go/cloud_integration_test.go
// for the full pattern writeup, repeated only briefly here) to
// examples/retry-go. Gated behind the cloudintegration build tag; makes
// a REAL, billed, synchronous Lambda Invoke against a REAL deployed
// function in account 730758745077 (region us-east-1), followed by real
// GetDurableExecutionHistory-backed polling. Not wired into any CI
// workflow.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// retryGoExampleFunctionARN is the real, deployed function's qualified
// version-1 ARN, confirmed live via a real smoke-test Invoke immediately
// before writing this test (see this repo's own deployment notes).
const retryGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:retry-go-example:1"

// TestCloudTestRunner_Handler_RealDeployedFunction runs a fast,
// non-suspending happy path (a request that succeeds on its very first
// attempt, so no retry delay/suspension is exercised) against the real
// deployed retry-go-example function.
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

	runner := dtesting.NewCloudTestRunner[FlakyRequestEvent, FlakyRequestResult](
		retryGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := FlakyRequestEvent{RequestID: "cloud-1", PresetFailUntilAttempt: 1, CustomFailUntilAttempt: 1}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	presetOp, ok := res.GetOperation("call-flaky-dependency-preset")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'call-flaky-dependency-preset' in the real, cloud-polled operation log")
	}
	if presetOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", presetOp.GetType())
	}
	if presetOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'call-flaky-dependency-preset' SUCCEEDED, got %s", presetOp.GetStatus())
	}
}
