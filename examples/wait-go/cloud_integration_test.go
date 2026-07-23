//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring (see examples/simple-step-go/cloud_integration_test.go
// for the full pattern writeup) to examples/wait-go. Gated behind the
// cloudintegration build tag; makes a REAL, billed, synchronous Lambda
// Invoke against a REAL deployed function in account 730758745077
// (region us-east-1). Not wired into any CI workflow.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// waitGoExampleFunctionARN is the real, deployed function's qualified
// version-1 ARN, confirmed live via a real smoke-test Invoke immediately
// before writing this test.
const waitGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:wait-go-example:1"

// TestCloudTestRunner_BasicHandler_RealDeployedFunction runs
// BasicHandler's 1-second Wait against the real deployed
// wait-go-example function - a real, synchronous Invoke blocks for the
// whole 1-second Wait duration, so this stays a single Run call (no
// RunUntilCallback/suspension-driving needed).
func TestCloudTestRunner_BasicHandler_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[CoolDownEvent, CoolDownResult](
		waitGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	res, err := runner.Run(ctx, "", CoolDownEvent{OrderID: "cloud-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	waitOp, ok := res.GetOperation("cool-down")
	if !ok {
		t.Fatal("expected to find a WAIT operation named 'cool-down' in the real, cloud-polled operation log")
	}
	if waitOp.GetType() != types.OperationTypeWait {
		t.Fatalf("expected WAIT type, got %s", waitOp.GetType())
	}
	if waitOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'cool-down' WAIT SUCCEEDED, got %s", waitOp.GetStatus())
	}
}
