//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/parallel-virtual-context-go. Gated
// behind the cloudintegration build tag; makes a REAL, billed,
// synchronous Lambda Invoke against a REAL deployed function in account
// 730758745077 (region us-east-1). Not wired into any CI workflow.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// parallelVirtualContextGoExampleFunctionARN is the real, deployed
// function's qualified version-1 ARN, confirmed live via a real
// smoke-test Invoke immediately before writing this test.
const parallelVirtualContextGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:parallel-virtual-context-go-example:1"

func TestCloudTestRunner_ParallelVirtualContextHandler_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[InventoryCheckEvent, InventoryCheckResult](
		parallelVirtualContextGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := InventoryCheckEvent{OrderID: "cloud-1", Warehouses: []string{"east", "west"}}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	parallelOp, ok := res.GetOperation("check-warehouses")
	if !ok {
		t.Fatal("expected to find the outer Parallel context operation 'check-warehouses' in the real, cloud-polled operation log")
	}
	if parallelOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'check-warehouses' SUCCEEDED, got %s", parallelOp.GetStatus())
	}

	// Confirm FLAT nesting's real effect against the REAL backend: zero
	// per-branch ParallelBranch contexts, and every inner Step's own
	// ParentID points directly at the outer Parallel context.
	var stepCount int
	for _, op := range res.GetOperations() {
		if op.GetType() != types.OperationTypeContext {
			if op.GetType() == types.OperationTypeStep {
				stepCount++
				if op.GetParentID() != parallelOp.GetID() {
					t.Errorf("expected each branch's inner Step to report ParentID=%s, got ParentID=%s", parallelOp.GetID(), op.GetParentID())
				}
			}
			continue
		}
		if op.GetName() != "check-warehouses" {
			t.Errorf("expected zero per-branch ParallelBranch contexts under FLAT nesting against the REAL backend, found one: %+v", op)
		}
	}
	if stepCount != 2 {
		t.Fatalf("expected exactly 2 Step operations (one per warehouse), got %d", stepCount)
	}
}
