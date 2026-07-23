// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
//
// This example closes docs/ts-sdk-examples-comparison.md's
// "parallel-processing" catalog entry (the TS SDK's top-level `parallel`
// example group) with the EXACT catalog name
// (examples/parallel-processing-go), distinct from the pre-existing
// examples/map-parallel-go: that example demonstrates Map AND Parallel
// TOGETHER in one combined order-checkout scenario (pricing orders via
// Map, then two verification branches via Parallel/All) - useful as a
// "how do these compose" reference, but not a focused "learn Parallel by
// itself" one. This example deliberately has NO Map anywhere in it: its
// entire job is operations.Parallel/All fanning out over several
// independent branches on their own, so a reader looking specifically for
// "how does Parallel work" is not forced to also understand Map first to
// follow the example.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// OrderEvent is this example's input shape: a single order to run
// concurrent, independent checks against.
type OrderEvent struct {
	OrderID string  `json:"orderId"`
	SKU     string  `json:"sku"`
	Amount  float64 `json:"amount"`
}

// OrderResult is this example's output shape: the aggregated results of
// every independent check, all of which ran concurrently via a single
// operations.All call.
type OrderResult struct {
	OrderID          string `json:"orderId"`
	FraudCleared     bool   `json:"fraudCleared"`
	InventoryInStock bool   `json:"inventoryInStock"`
	AddressValid     bool   `json:"addressValid"`
	PaymentAuthed    bool   `json:"paymentAuthed"`
}

// errFraudCheckFailed is returned by the fraud-check branch's own Step
// when an order's amount exceeds the (deliberately simplistic) fraud
// threshold used by this example. A genuine Step error - not merely a
// falsy bool result - so that operations.All's "any branch fails, the
// whole batch fails" semantics (batch.go's own doc: "returns the
// successful results only if every branch succeeds; otherwise ...
// *AggregateError") have something real to demonstrate; see
// TestHandler_FraudCheckFailsWholeBatch.
var errFraudCheckFailed = fmt.Errorf("fraud check failed: amount exceeds threshold")

// handler demonstrates operations.Parallel (via its typed convenience
// wrapper operations.All - see batch.go's doc: "All is a convenience
// wrapper over Parallel for the common case where every branch returns
// the same type and any branch failure should fail the whole batch")
// fanning out over FOUR independent branches for a single order, each
// modeling a call to a distinct external service that has no dependency
// on any of the others: fraud detection, inventory, address validation,
// and payment authorization. In a real system these would be four
// separate network calls (to a fraud-detection API, an inventory
// service, an address-validation service, and a payment processor) that
// have no reason to run sequentially - operations.Parallel checkpoints
// each branch independently (CONTEXT/PARALLEL nesting four
// PARALLEL_BRANCH children, per the confirmed real-backend flowchart,
// internal SDK Operation Diagrams design doc, "Parallel") so a
// suspend-and-resume mid-batch only re-runs whichever branches hadn't
// yet completed - already-completed ones replay-skip via their own
// child context's checkpoint, exactly like every other Parallel/Map
// example in this repo.
//
// Unlike examples/map-parallel-go's operations.All call (which uses
// Parallel for exactly two verification branches ALONGSIDE an unrelated
// Map fan-out over an order batch, in service of demonstrating how the
// two operations COMPOSE), this handler's entire body is a single
// operations.All call and nothing else - the minimal, standalone "learn
// Parallel" reference a real example catalog entry should be, matching
// what the "parallel-processing" catalog name implies without asking a
// reader to also understand Map first.
func handler(event OrderEvent, dc types.DurableContext) (OrderResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID, "sku": event.SKU})

	branches := []func(types.DurableContext) (bool, error){
		func(child types.DurableContext) (bool, error) {
			return operations.Step(child, "fraud-check", func(sc types.StepContext) (bool, error) {
				// In production this would call a real fraud-detection
				// service, e.g. Amazon Fraud Detector. A genuine Step
				// error (not just a falsy result) for amounts over the
				// threshold, so this branch can fail for real - see
				// errFraudCheckFailed's doc for why.
				sc.Logger().Info("checking for fraud", map[string]any{"orderId": event.OrderID})
				if event.Amount >= 10000 {
					return false, errFraudCheckFailed
				}
				return true, nil
			})
		},
		func(child types.DurableContext) (bool, error) {
			return operations.Step(child, "inventory-check", func(sc types.StepContext) (bool, error) {
				// In production this would call a real inventory service.
				sc.Logger().Info("checking inventory", map[string]any{"sku": event.SKU})
				return true, nil
			})
		},
		func(child types.DurableContext) (bool, error) {
			return operations.Step(child, "address-validation", func(sc types.StepContext) (bool, error) {
				// In production this would call a real address-validation
				// service, e.g. Amazon Location Service.
				sc.Logger().Info("validating shipping address", map[string]any{"orderId": event.OrderID})
				return true, nil
			})
		},
		func(child types.DurableContext) (bool, error) {
			return operations.Step(child, "payment-authorization", func(sc types.StepContext) (bool, error) {
				// In production this would call a real payment processor
				// to place an authorization hold.
				sc.Logger().Info("authorizing payment", map[string]any{"orderId": event.OrderID, "amount": event.Amount})
				return true, nil
			})
		},
	}

	// All four branches run concurrently; operations.All returns their
	// results in the SAME order the branches slice was given, regardless
	// of which branch's underlying Step actually finished first (per
	// batch.go's own doc on result ordering), so indexing into the
	// returned slice by position is safe and deterministic.
	results, err := operations.All(dc, "order-checks", branches)
	if err != nil {
		return OrderResult{}, fmt.Errorf("order %s: running concurrent checks: %w", event.OrderID, err)
	}

	dc.Logger().Info("handler completed", map[string]any{"orderId": event.OrderID})
	return OrderResult{
		OrderID:          event.OrderID,
		FraudCleared:     results[0],
		InventoryInStock: results[1],
		AddressValid:     results[2],
		PaymentAuthed:    results[3],
	}, nil
}
