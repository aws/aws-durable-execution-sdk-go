//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/custom-config-go. Gated behind the
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

// customConfigGoExampleFunctionARN is the real, deployed function's
// qualified version-1 ARN, confirmed live via a real smoke-test Invoke
// immediately before writing this test.
const customConfigGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:custom-config-go-example:1"

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

	runner := dtesting.NewCloudTestRunner[InventorySnapshotEvent, InventorySnapshotResult](
		customConfigGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	res, err := runner.Run(ctx, "", InventorySnapshotEvent{WarehouseID: "cloud-warehouse-1"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	snapshotOp, ok := res.GetOperation("snapshot-inventory")
	if !ok {
		t.Fatal("expected to find a STEP operation named 'snapshot-inventory' in the real, cloud-polled operation log")
	}
	if snapshotOp.GetType() != types.OperationTypeStep {
		t.Fatalf("expected STEP type, got %s", snapshotOp.GetType())
	}
	if snapshotOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'snapshot-inventory' SUCCEEDED, got %s", snapshotOp.GetStatus())
	}
}
