// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, satisfying docs/remaining-work.md's completion
// criteria: asserting on checkpointed CONTEXT/MAP, CONTEXT/MAP_ITERATION,
// CONTEXT/PARALLEL, and CONTEXT/PARALLEL_BRANCH operations, not just the
// final result.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestHandler_PricesAndVerifiesConcurrently(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderBatchEvent{OrderIDs: []string{"order-1", "order-22", "order-333"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[OrderBatchResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if len(out.PricedOrders) != 3 {
		t.Fatalf("expected 3 priced orders, got %d", len(out.PricedOrders))
	}
	if !out.FraudCheckPassed || !out.InventoryVerified {
		t.Fatalf("expected both verifications to pass, got fraud=%v inventory=%v", out.FraudCheckPassed, out.InventoryVerified)
	}

	// Assert on the outer CONTEXT/MAP operation.
	mapOp, ok := result.GetOperation("price-orders")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'price-orders'")
	}
	if mapOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", mapOp.GetType())
	}
	if mapOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected map SUCCEEDED, got %s", mapOp.GetStatus())
	}

	// Assert on one MAP_ITERATION and its nested step.
	iterOp, ok := result.GetOperation("price-orders[0]")
	if !ok {
		t.Fatal("expected to find a CONTEXT/MAP_ITERATION operation named 'price-orders[0]'")
	}
	if iterOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected iteration 0 SUCCEEDED, got %s", iterOp.GetStatus())
	}

	// Assert on the outer CONTEXT/PARALLEL operation and its branches.
	parallelOp, ok := result.GetOperation("pre-checkout-verifications")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'pre-checkout-verifications'")
	}
	if parallelOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected parallel SUCCEEDED, got %s", parallelOp.GetStatus())
	}

	fraudBranch, ok := result.GetOperation("pre-checkout-verifications[0]")
	if !ok {
		t.Fatal("expected to find a CONTEXT/PARALLEL_BRANCH operation named 'pre-checkout-verifications[0]'")
	}
	if fraudBranch.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected fraud-check branch SUCCEEDED, got %s", fraudBranch.GetStatus())
	}

	inventoryBranch, ok := result.GetOperation("pre-checkout-verifications[1]")
	if !ok {
		t.Fatal("expected to find a CONTEXT/PARALLEL_BRANCH operation named 'pre-checkout-verifications[1]'")
	}
	if inventoryBranch.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected inventory-check branch SUCCEEDED, got %s", inventoryBranch.GetStatus())
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): asserts on the deterministic SHAPE of the full operation log
	// - the outer CONTEXT/MAP wrapping three CONTEXT/MAP_ITERATION
	// children (each nesting its own "price-lookup" STEP), followed by
	// the outer CONTEXT/PARALLEL wrapping two CONTEXT/PARALLEL_BRANCH
	// children (each nesting its own STEP) - against a committed golden
	// file, in one assertion, rather than requiring a bespoke
	// GetOperation/GetChildOperations check per structural property the
	// way the individual assertions above do. This is the first example
	// in this repo exercising CONCURRENT branches/iterations (Map items
	// and Parallel branches run on separate goroutines), so this golden
	// file specifically also guards against non-deterministic ORDERING
	// leaking into the signature despite Map/Parallel's own concurrency
	// - EventSignatures derives Index from each operation's own
	// hierarchical step ID (claimed up front, in index order, before any
	// branch goroutine is spawned - see §1 task 5's bug #3 writeup in
	// this document), not from goroutine completion order or Go's
	// unordered map iteration, so the signature is expected to be
	// stable across repeated runs even though the underlying goroutine
	// scheduling is not. Verified via `go test -count=10` (and higher)
	// specifically because of this concurrency concern - see this
	// task's writeup in docs/remaining-work.md §7 task 17c for the
	// result. Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/map-parallel-go/... -run TestHandler_PricesAndVerifiesConcurrently
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_PricesAndVerifiesConcurrently.history.json")
}

func TestHandler_EmptyOrderBatch(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderBatchEvent{OrderIDs: []string{}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[OrderBatchResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if len(out.PricedOrders) != 0 {
		t.Fatalf("expected 0 priced orders, got %d", len(out.PricedOrders))
	}

	// Event-history/signature assertion (docs/remaining-work.md §7 task
	// 17c): a DISTINCT golden file from
	// TestHandler_PricesAndVerifiesConcurrently's, since an empty order
	// batch produces a meaningfully different operation-log shape - the
	// CONTEXT/MAP operation still runs (and succeeds, over zero items),
	// but has NO CONTEXT/MAP_ITERATION children at all, unlike the
	// three-item case above. The CONTEXT/PARALLEL verifications and
	// their two branches are unaffected by the batch being empty, since
	// they don't depend on OrderIDs, so they still appear in this
	// golden file exactly as they do in the non-empty case - this
	// specifically pins that an empty Map does not somehow also skip or
	// alter the unrelated Parallel call later in the same handler.
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/map-parallel-go/... -run TestHandler_EmptyOrderBatch
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_EmptyOrderBatch.history.json")
}

// TestHandler_EmptyVerificationBranches exercises operations.All/Parallel
// with ZERO branches - a genuinely different code path through the SAME
// runBatch/batchScheduler machinery TestHandler_PricesAndVerifiesConcurrently
// already exercises with 2 branches, and distinct from
// TestHandler_EmptyOrderBatch's zero-ITEM Map case above (this test still
// prices a real order via Map, isolating the Parallel-side zero-branches
// scenario specifically). See docs/ts-sdk-examples-comparison.md's
// "parallel/empty" finding (gap 7/8) and OrderBatchEvent.SkipVerifications'
// own doc comment for why this is expressed as a bool flag rather than an
// empty selection list.
func TestHandler_EmptyVerificationBranches(t *testing.T) {
	runner := dtesting.New(handler, nil)

	result, err := runner.Run(OrderBatchEvent{OrderIDs: []string{"order-1"}, SkipVerifications: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[OrderBatchResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if len(out.PricedOrders) != 1 {
		t.Fatalf("expected 1 priced order (Map side unaffected by SkipVerifications), got %d", len(out.PricedOrders))
	}
	// Neither verification ran - see OrderBatchResult's own doc for why
	// both fields staying at their Go zero value (false) is the correct,
	// deliberate behavior here, not a bug.
	if out.FraudCheckPassed || out.InventoryVerified {
		t.Fatalf("expected both verification fields to remain false (neither check ran), got FraudCheckPassed=%v InventoryVerified=%v", out.FraudCheckPassed, out.InventoryVerified)
	}

	parallelOp, ok := result.GetOperation("pre-checkout-verifications")
	if !ok {
		t.Fatal("expected to find a CONTEXT operation named 'pre-checkout-verifications' even with zero branches")
	}
	if parallelOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected 'pre-checkout-verifications' to be a CONTEXT operation, got %s", parallelOp.GetType())
	}
	if parallelOp.GetStatus() != types.OperationStatusSucceeded {
		t.Fatalf("expected the outer PARALLEL context to SUCCEED even with zero branches, got %s", parallelOp.GetStatus())
	}
	children := result.GetChildOperations(parallelOp.GetID())
	if len(children) != 0 {
		t.Fatalf("expected ZERO PARALLEL_BRANCH children, got %d", len(children))
	}

	// Event-history/signature assertion: a THIRD, distinct golden-file
	// shape from this file's other two tests - a CONTEXT/PARALLEL entry
	// with NO nested children at all, alongside a normal single-item
	// CONTEXT/MAP with its one MAP_ITERATION child.
	// Regenerate with:
	// UPDATE_GOLDEN=1 go test ./examples/map-parallel-go/... -run TestHandler_EmptyVerificationBranches
	dtesting.AssertEventSignatures(t, result, "testdata/TestHandler_EmptyVerificationBranches.history.json")
}
