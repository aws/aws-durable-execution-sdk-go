//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/logging-go. Gated behind the
// cloudintegration build tag; makes a REAL, billed, synchronous Lambda
// Invoke against a REAL deployed function in account 730758745077
// (region us-east-1). Not wired into any CI workflow.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// loggingGoExampleFunctionARN is the real, deployed function's qualified
// version-1 ARN, confirmed live via a real smoke-test Invoke immediately
// before writing this test.
const loggingGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:logging-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[ReportEvent, ReportResult](
		loggingGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	res, err := runner.Run(ctx, "", ReportEvent{ReportID: "cloud-report-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	gatherOp, ok := res.GetOperation("gather-data")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'gather-data' in the real, cloud-polled operation log")
	}
	if gatherOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'gather-data' SUCCEEDED, got %s", gatherOp.GetStatus())
	}

	summarizeOp, ok := res.GetOperation("summarize-data")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'summarize-data' in the real, cloud-polled operation log")
	}
	if summarizeOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'summarize-data' SUCCEEDED, got %s", summarizeOp.GetStatus())
	}
}
