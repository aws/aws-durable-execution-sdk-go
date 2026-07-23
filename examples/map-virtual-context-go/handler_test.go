// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, confirming the FLAT-nesting checkpoint reduction is
// real: zero MapIteration CONTEXT operations, and every inner Step's own
// ParentID points directly at the outer Map context - mirroring
// pkg/durable/durable_flat_nesting_test.go's own established assertion
// pattern for this SDK feature.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestMapVirtualContextHandler_SkipsPerIterationContexts(t *testing.T) {
	runner := dtesting.New(MapVirtualContextHandler, nil)

	result, err := runner.Run(PriceCheckEvent{OrderID: "ord-001", SKUs: []string{"SKU-A", "SKU-BB", "SKU-CCC"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[PriceCheckResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	wantPrices := []float64{5 * 9.99, 6 * 9.99, 7 * 9.99}
	if len(out.Prices) != len(wantPrices) {
		t.Fatalf("expected %d prices, got %d", len(wantPrices), len(out.Prices))
	}
	for i, want := range wantPrices {
		if out.Prices[i] != want {
			t.Errorf("price[%d]: expected %v, got %v", i, want, out.Prices[i])
		}
	}

	mapOp, ok := result.GetOperation("price-lookup")
	if !ok {
		t.Fatal("expected to find the outer Map context operation 'price-lookup'")
	}
	if mapOp.GetType() != types.OperationTypeContext {
		t.Fatalf("expected CONTEXT type, got %s", mapOp.GetType())
	}

	var stepCount int
	for _, op := range result.GetOperations() {
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
			// Any OTHER context besides the outer Map itself would be a
			// per-iteration MapIteration context - FLAT nesting's entire
			// point is that none exist. (Excludes the root EXECUTION
			// operation, which is a different OperationType entirely and
			// already skipped by the != Context check above.)
			t.Errorf("expected zero per-iteration MapIteration contexts under FLAT nesting, found one: %+v", op)
		}
	}
	if stepCount != 3 {
		t.Fatalf("expected exactly 3 Step operations (one per SKU), got %d", stepCount)
	}

	dtesting.AssertEventSignatures(t, result, "testdata/TestMapVirtualContextHandler_SkipsPerIterationContexts.history.json")
}
