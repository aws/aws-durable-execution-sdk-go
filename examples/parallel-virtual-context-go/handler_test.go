// handler_test.go exercises this example's handler against the SDK's
// LocalTestRunner, confirming the FLAT-nesting checkpoint reduction is
// real for Parallel too - mirroring map-virtual-context-go's own test.
package main

import (
	"testing"

	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestParallelVirtualContextHandler_SkipsPerBranchContexts(t *testing.T) {
	runner := dtesting.New(ParallelVirtualContextHandler, nil)

	result, err := runner.Run(InventoryCheckEvent{OrderID: "ord-001", Warehouses: []string{"east", "west-coast"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}

	out, err := dtesting.GetResult[InventoryCheckResult](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// "east" (4 chars * 10) + "west-coast" (10 chars * 10) = 40 + 100 = 140.
	if out.TotalUnitsFound != 140 {
		t.Fatalf("expected TotalUnitsFound=140, got %d", out.TotalUnitsFound)
	}

	parallelOp, ok := result.GetOperation("check-warehouses")
	if !ok {
		t.Fatal("expected to find the outer Parallel context operation 'check-warehouses'")
	}

	var stepCount int
	for _, op := range result.GetOperations() {
		if op.GetType() != types.OperationTypeContext {
			if op.GetType() == types.OperationTypeStep {
				stepCount++
				if op.GetParentID() != parallelOp.GetID() {
					t.Errorf("expected each branch's inner Step to report ParentID=%s (the outer Parallel context's own ID), got ParentID=%s", parallelOp.GetID(), op.GetParentID())
				}
			}
			continue
		}
		if op.GetName() != "check-warehouses" {
			t.Errorf("expected zero per-branch ParallelBranch contexts under FLAT nesting, found one: %+v", op)
		}
	}
	if stepCount != 2 {
		t.Fatalf("expected exactly 2 Step operations (one per warehouse), got %d", stepCount)
	}

	dtesting.AssertEventSignatures(t, result, "testdata/TestParallelVirtualContextHandler_SkipsPerBranchContexts.history.json")
}
