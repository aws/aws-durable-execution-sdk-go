//go:build cloudintegration

// cloud_integration_test.go extends this repo's established
// CloudTestRunner wiring to examples/map-virtual-context-go. Gated
// behind the cloudintegration build tag; makes a REAL, billed,
// synchronous Lambda Invoke against a REAL deployed function in account
// 730758745077 (region us-east-1). Not wired into any CI workflow.
//
// Confirms FLAT nesting's real checkpoint-count reduction against the
// REAL backend, not just the fake in-memory client - the actual point
// this example exists to demonstrate.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// mapVirtualContextGoExampleFunctionARN is the real, deployed function's
// qualified version-1 ARN, confirmed live via a real smoke-test Invoke
// immediately before writing this test.
const mapVirtualContextGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:map-virtual-context-go-example:1"

func TestCloudTestRunner_MapVirtualContextHandler_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active?)", err)
	}
	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[PriceCheckEvent, PriceCheckResult](
		mapVirtualContextGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := PriceCheckEvent{OrderID: "cloud-1", SKUs: []string{"SKU-A", "SKU-BB"}}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	mapOp, ok := res.GetOperation("price-lookup")
	if !ok {
		t.Fatal("expected to find the outer Map context operation 'price-lookup' in the real, cloud-polled operation log")
	}
	if mapOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", mapOp.GetType())
	}
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'price-lookup' SUCCEEDED, got %s", mapOp.GetStatus())
	}

	// Confirm FLAT nesting's real effect against the REAL backend: zero
	// per-iteration MapIteration contexts, and every inner Step's own
	// ParentID points directly at the outer Map context.
	var stepCount int
	for _, op := range res.GetOperations() {
		if op.GetType() != types.OperationTypeContext {
			if op.GetType() == types.OperationTypeStep {
				stepCount++
				if op.GetParentID() != mapOp.GetID() {
					t.Errorf("expected each item's inner Step to report ParentID=%s (the outer Map context's own ID), got ParentID=%s", mapOp.GetID(), op.GetParentID())
				}
			}
			continue
		}
		if op.GetName() != "price-lookup" {
			t.Errorf("expected zero per-iteration MapIteration contexts under FLAT nesting against the REAL backend, found one: %+v", op)
		}
	}
	if stepCount != 2 {
		t.Fatalf("expected exactly 2 Step operations (one per SKU), got %d", stepCount)
	}
}
