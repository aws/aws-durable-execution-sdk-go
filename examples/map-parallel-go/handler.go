// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// OrderBatchEvent is this example's input shape: a batch of order IDs to
// price-check concurrently, plus a fixed set of independent pre-checkout
// verifications to run alongside them.
//
// SkipVerifications, when true, demonstrates operations.All/Parallel with
// ZERO branches instead of the normal fixed 2 (fraud-check,
// inventory-check) - see docs/ts-sdk-examples-comparison.md's
// "parallel/empty" finding (gap 7/8): map-parallel-go already had an
// empty-Map scenario (OrderIDs: []) via TestHandler_EmptyOrderBatch, but
// nothing exercised a zero-branch Parallel/All, which is a genuinely
// different code path through the SAME runBatch/batchScheduler machinery
// (see batch.go's runBatch: "if n == 0 { return results, true }" is the
// exact early-return this field's own scenario is designed to exercise).
// Deliberately a bool flag on the event rather than, say, an empty
// verifications-selection list: this handler's verification branches
// are a fixed, hardcoded pair with no natural "which ones to run"
// selection semantics to expose, so a single on/off switch is the
// minimal, event-shape-consistent mechanism - mirroring how
// OrderIDs: []  already toggles Map's own zero-item case via the shape
// of its own existing field, without needing a parallel boolean for
// Map. Omitting this field (the Go zero value, false) preserves every
// pre-existing test/deployment's exact behavior unchanged - this is a
// purely additive change.
type OrderBatchEvent struct {
	OrderIDs          []string `json:"orderIds"`
	SkipVerifications bool     `json:"skipVerifications,omitempty"`
}

// OrderBatchResult is this example's output shape.
//
// FraudCheckPassed/InventoryVerified default to their Go zero value
// (false) when SkipVerifications is true and zero verification branches
// actually ran - chosen deliberately, and documented here rather than
// left to infer: "false" (as opposed to, say, defaulting both to true)
// correctly reflects that NEITHER check actually ran, so nothing was
// actually verified/passed - a caller inspecting these fields after a
// SkipVerifications:true execution must not read a bare `false` as "the
// fraud check ran and failed"; the same ambiguity operations.All itself
// already has for a zero-branch call (an empty []TOut result, not a
// sentinel), which this handler's own result shape inherits rather than
// papering over with a separate "verificationsRan bool" field, since
// this example's job is to demonstrate the operation's real zero-branch
// behavior, not to design around it.
type OrderBatchResult struct {
	PricedOrders      []PricedOrder `json:"pricedOrders"`
	FraudCheckPassed  bool          `json:"fraudCheckPassed"`
	InventoryVerified bool          `json:"inventoryVerified"`
}

// PricedOrder is one Map iteration's result.
type PricedOrder struct {
	OrderID string  `json:"orderId"`
	Price   float64 `json:"price"`
}

// handler demonstrates both operations.Map and operations.Parallel in a
// single workflow:
//
//   - operations.Map prices every order ID concurrently, each within its
//     own isolated child context, demonstrating the CONTEXT/MAP +
//     CONTEXT/MAP_ITERATION checkpoint lifecycle confirmed via the
//     internal SDK Operation Diagrams design doc.
//   - operations.Parallel runs two independent pre-checkout verifications
//     (a fraud check and an inventory check) concurrently, demonstrating
//     the structurally identical CONTEXT/PARALLEL +
//     CONTEXT/PARALLEL_BRANCH lifecycle.
//
// Both operations checkpoint each item/branch independently, so a
// suspend-and-resume mid-batch (e.g. if a branch used WaitForCallback)
// only re-runs whichever items/branches hadn't yet completed - already-
// completed ones replay-skip via their own child context's checkpoint.
func handler(event OrderBatchEvent, dc types.DurableContext) (OrderBatchResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderCount": len(event.OrderIDs)})

	pricedBatch, err := operations.Map(dc, "price-orders", event.OrderIDs,
		func(child types.DurableContext, orderID string, index int) (PricedOrder, error) {
			price, err := operations.Step(child, "price-lookup", func(sc types.StepContext) (float64, error) {
				// In production this would call a real pricing service.
				return 9.99 + float64(len(orderID)), nil
			})
			if err != nil {
				return PricedOrder{}, fmt.Errorf("pricing order %s: %w", orderID, err)
			}
			return PricedOrder{OrderID: orderID, Price: price}, nil
		},
	)
	if err != nil {
		return OrderBatchResult{}, fmt.Errorf("pricing orders: %w", err)
	}

	// Branches is a plain Go slice constructed BEFORE calling
	// operations.All, so the zero-branch case (SkipVerifications: true)
	// is simply an empty slice literal - no special-casing needed in the
	// call itself; runBatch's own "if n == 0 { return results, true }"
	// early return (batch.go) handles a zero-length branch slice
	// correctly at the SDK level already. This variable exists only so
	// the conditional construction reads clearly; it has no other
	// purpose.
	var branches []func(types.DurableContext) (bool, error)
	if !event.SkipVerifications {
		branches = []func(types.DurableContext) (bool, error){
			func(child types.DurableContext) (bool, error) {
				return operations.Step(child, "fraud-check", func(sc types.StepContext) (bool, error) {
					// In production this would call a real fraud-detection service.
					return true, nil
				})
			},
			func(child types.DurableContext) (bool, error) {
				return operations.Step(child, "inventory-check", func(sc types.StepContext) (bool, error) {
					// In production this would call a real inventory service.
					return true, nil
				})
			},
		}
	}

	verifications, err := operations.All(dc, "pre-checkout-verifications", branches)
	if err != nil {
		return OrderBatchResult{}, fmt.Errorf("running pre-checkout verifications: %w", err)
	}

	priced := make([]PricedOrder, len(pricedBatch.Items))
	for i, item := range pricedBatch.Items {
		priced[i] = item.Value
	}

	dc.Logger().Info("handler completed", map[string]any{"pricedCount": len(priced)})

	// FraudCheckPassed/InventoryVerified stay at their Go zero value
	// (false) when SkipVerifications is true - see OrderBatchResult's
	// own doc for why this is the deliberate, correct choice rather than
	// defaulting either field to true.
	result := OrderBatchResult{PricedOrders: priced}
	if len(verifications) > 0 {
		result.FraudCheckPassed = verifications[0]
		result.InventoryVerified = verifications[1]
	}
	return result, nil
}
