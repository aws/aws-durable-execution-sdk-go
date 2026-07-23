// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/wait-for-callback-go's
// own handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// ShipmentEvent is this example's input shape.
type ShipmentEvent struct {
	OrderID string `json:"orderId"`
}

// ShipmentResult is this example's output shape.
type ShipmentResult struct {
	OrderID          string `json:"orderId"`
	InventoryChecked bool   `json:"inventoryChecked"`
	CarrierStatus    string `json:"carrierStatus"`
}

// handler demonstrates operations.CreateCallback - the LOW-LEVEL
// primitive underlying WaitForCallback (see examples/wait-for-callback-go
// for that higher-level, single-call wrapper). Unlike WaitForCallback,
// CreateCallback returns the callback ID and a result channel
// IMMEDIATELY, without blocking - letting the caller do other durable
// work (here, an inventory-check Step) before ever awaiting the result.
// This is CreateCallback's own documented reason to exist over
// WaitForCallback: "prefer WaitForCallback unless you need to interleave
// callback registration with other durable operations before
// suspending" (see operations.CreateCallback's own doc comment).
//
// Scenario: an order-shipment workflow registers a callback for a
// third-party carrier's "package picked up" webhook, then - WHILE that
// callback is outstanding - runs its own inventory-check step. Only once
// both are done does it await the callback's result. This mirrors the
// JS reference SDK's own create-callback/mixed-ops example ("createCallback
// mixed with steps, waits, and other operations").
//
// The hand-off of the callback ID to the external carrier system happens
// via a Step (carrierRegistration below), exactly like WaitForCallback's
// own submitter - a Step's own retry/replay-safety guarantees make that
// hand-off safe to re-run on replay without registering the callback
// with the carrier twice.
func handler(event ShipmentEvent, dc types.DurableContext) (ShipmentResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderId": event.OrderID})

	resultCh, callbackID, err := operations.CreateCallback[string](dc, "carrier-pickup")
	if err != nil {
		return ShipmentResult{}, fmt.Errorf("order %s: creating carrier-pickup callback: %w", event.OrderID, err)
	}

	// Hand the callback ID off to the external carrier system via a Step
	// (replay-safe: this only actually registers with the carrier once,
	// even if the handler re-runs on replay before the callback resolves).
	_, err = operations.Step(dc, "register-with-carrier", func(sc types.StepContext) (string, error) {
		sc.Logger().Info("registering pickup webhook with carrier", map[string]any{
			"orderId":    event.OrderID,
			"callbackId": callbackID,
		})
		return callbackID, nil
	})
	if err != nil {
		return ShipmentResult{}, fmt.Errorf("order %s: registering with carrier: %w", event.OrderID, err)
	}

	// While the carrier's webhook is outstanding, do other durable work -
	// this is exactly what CreateCallback enables that WaitForCallback's
	// single blocking call does not.
	_, err = operations.Step(dc, "check-inventory", func(sc types.StepContext) (bool, error) {
		sc.Logger().Info("checking inventory levels", map[string]any{"orderId": event.OrderID})
		return true, nil
	})
	if err != nil {
		return ShipmentResult{}, fmt.Errorf("order %s: checking inventory: %w", event.OrderID, err)
	}

	// Only now block on the carrier's callback result. AwaitCallback, not
	// a raw channel receive - see operations.CreateCallback's own doc for
	// why a raw receive is a real correctness bug (it prevents the
	// execution from suspending while genuinely waiting on an external
	// system).
	result := operations.AwaitCallback(dc, resultCh)
	if result.Err != nil {
		return ShipmentResult{}, fmt.Errorf("order %s: carrier pickup callback: %w", event.OrderID, result.Err)
	}

	dc.Logger().Info("handler completed", map[string]any{"carrierStatus": result.Value})
	return ShipmentResult{OrderID: event.OrderID, InventoryChecked: true, CarrierStatus: result.Value}, nil
}
