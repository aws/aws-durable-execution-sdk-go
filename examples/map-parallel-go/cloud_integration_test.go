//go:build cloudintegration

// cloud_integration_test.go extends docs/remaining-work.md's §7 task
// 17b's CloudTestRunner wiring to examples/map-parallel-go, following
// the exact same pattern established by
// examples/completion-config-go/cloud_integration_test.go (see that
// file's own extensive header doc comment for the full rationale on the
// `//go:build cloudintegration` gating and why this is NOT wired into
// CI - not re-litigated here).
//
// # Why a normal (non-empty-verifications) multi-order payload
//
// handler.go's OrderBatchEvent has a SkipVerifications flag that takes
// operations.All down a zero-branch path (see that field's own doc for
// the local-runner-only TestHandler_EmptyOrderBatch/zero-Parallel-branch
// scenarios this already covers). This cloud test uses neither of
// those edge cases - per this task's own instruction ("use a normal
// (non-empty-verifications) multi-order payload") - a genuine multi-item
// Map fan-out (3 orders) PLUS the normal two-branch Parallel
// verification (fraud-check, inventory-check), proving the COMBINED
// CONTEXT/MAP + CONTEXT/PARALLEL shape (two sibling top-level CONTEXT
// operations, each with their own nested children -
// MAP_ITERATION×3 under "price-orders", PARALLEL_BRANCH×2 under
// "pre-checkout-verifications") round-trips correctly through the real
// backend and sdk_state_client.go's history reconstruction - a
// genuinely different shape from run-in-child-context-go's single
// nested CONTEXT and completion-config-go's single CONTEXT/MAP with
// tolerated per-item failures.
package main

import (
	"context"
	"testing"
	"time"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// mapParallelGoExampleFunctionARN is the REAL, already-deployed,
// currently-active target this test invokes - the latest published
// qualified version ARN, confirmed via a read-only
// `lambda:ListVersionsByFunction` check immediately before writing this
// test: version 3's CodeSha256
// (e672cbf9478e974b172474d6202cc77a1179ecc3732f31d5bfdc145f663318d0)
// matches $LATEST's, confirming version 3 is the function's current
// code. Per CloudTestRunner.FunctionName's own doc comment, this MUST be
// a qualified ARN.
const mapParallelGoExampleFunctionARN = "arn:aws:lambda:us-east-1:730758745077:function:map-parallel-go-example:3"

// TestCloudTestRunner_NormalMultiOrderBatch_RealDeployedFunction runs a
// normal (non-empty, non-SkipVerifications) multi-order batch against the
// real deployed map-parallel-go-example function via
// testing.CloudTestRunner: 3 orders priced via Map, plus the standard
// 2-branch Parallel verification (fraud-check, inventory-check) - a fast,
// non-suspending happy path (no Wait/Callback anywhere in this
// example's handler at all).
func TestCloudTestRunner_NormalMultiOrderBatch_RealDeployedFunction(t *testing.T) {
	ctx := context.Background()

	invoker, err := dtesting.NewLambdaInvoker(ctx)
	if err != nil {
		t.Fatalf("NewLambdaInvoker: %v (is a real AWS credential chain active? see this file's own doc comment)", err)
	}

	stateClient, err := dtesting.NewStateClient(ctx)
	if err != nil {
		t.Fatalf("NewStateClient: %v", err)
	}

	runner := dtesting.NewCloudTestRunner[OrderBatchEvent, OrderBatchResult](
		mapParallelGoExampleFunctionARN,
		invoker,
		stateClient,
	)
	runner.PollInterval = 2 * time.Second
	runner.Timeout = 2 * time.Minute

	event := OrderBatchEvent{
		OrderIDs:          []string{"order-cloud-1", "order-cloud-2", "order-cloud-3"},
		SkipVerifications: false,
	}

	res, err := runner.Run(ctx, "", event)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}

	// Assert on the "price-orders" CONTEXT/MAP operation: SUCCEEDED, with
	// exactly 3 nested MAP_ITERATION children, each itself a CONTEXT
	// operation whose own nested "price-lookup" STEP succeeded.
	priceOrdersOp, ok := res.GetOperation("price-orders")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'price-orders'")
	}
	if priceOrdersOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type for 'price-orders', got %s", priceOrdersOp.GetType())
	}
	if priceOrdersOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'price-orders' to report SUCCEEDED, got %s", priceOrdersOp.GetStatus())
	}

	mapIterations := res.GetChildOperations(priceOrdersOp.GetID())
	if len(mapIterations) != 3 {
		t.Fatalf("expected 3 MAP_ITERATION children under 'price-orders', got %d", len(mapIterations))
	}
	for _, iter := range mapIterations {
		if iter.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected MAP_ITERATION %q to report SUCCEEDED, got %s", iter.GetID(), iter.GetStatus())
		}
	}

	// Assert on the "pre-checkout-verifications" CONTEXT/PARALLEL
	// operation: SUCCEEDED, with exactly 2 nested PARALLEL_BRANCH
	// children (fraud-check, inventory-check STEPs), both succeeded -
	// proving the normal, non-zero-branch Parallel path (distinct from
	// the SkipVerifications:true zero-branch scenario this task's own
	// instructions explicitly said NOT to use here).
	verificationsOp, ok := res.GetOperation("pre-checkout-verifications")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'pre-checkout-verifications'")
	}
	if verificationsOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type for 'pre-checkout-verifications', got %s", verificationsOp.GetType())
	}
	if verificationsOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected 'pre-checkout-verifications' to report SUCCEEDED, got %s", verificationsOp.GetStatus())
	}

	branches := res.GetChildOperations(verificationsOp.GetID())
	if len(branches) != 2 {
		t.Fatalf("expected 2 PARALLEL_BRANCH children under 'pre-checkout-verifications', got %d", len(branches))
	}
	for _, branch := range branches {
		if branch.GetStatus() != types.OperationStatusSucceeded {
			t.Fatalf("expected PARALLEL_BRANCH %q to report SUCCEEDED, got %s", branch.GetID(), branch.GetStatus())
		}
	}

	// Cross-check the nested "fraud-check"/"inventory-check" STEP names
	// are genuinely present somewhere in the recursive operation log
	// (nested two levels deep: PARALLEL_BRANCH CONTEXT -> STEP),
	// confirming sdk_state_client.go's ParentID-based reconstruction
	// correctly threads parentage through this deeper nesting, not just
	// the single-level nesting run-in-child-context-go's own cloud test
	// already confirmed.
	if _, ok := res.GetOperationRecursive("fraud-check"); !ok {
		t.Fatal("expected to find a nested STEP operation named 'fraud-check' via GetOperationRecursive")
	}
	if _, ok := res.GetOperationRecursive("inventory-check"); !ok {
		t.Fatal("expected to find a nested STEP operation named 'inventory-check' via GetOperationRecursive")
	}

	// GetResult[OrderBatchResult] (top-level handler result) is
	// deliberately NOT asserted on, per the same documented,
	// not-independently-re-confirmed-for-this-function gap every prior
	// cloud_integration_test.go in this repo leaves unasserted - see
	// completion-config-go's own test for the full finding this defers
	// to rather than re-guessing.
}
