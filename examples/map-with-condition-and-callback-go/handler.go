// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
//
// This example closes docs/ts-sdk-examples-comparison.md's
// "map-with-condition-and-callback" catalog entry: a genuinely different
// COMBINATION from any single existing example in this repo. No existing
// example combines all three of operations.Map, operations.WaitForCondition,
// and operations.WaitForCallback in one workflow - map-parallel-go
// combines Map+Parallel (no Wait* operations at all), wait-for-callback-go
// and wait-for-condition-go each demonstrate exactly one Wait* operation
// in isolation (no Map), and completion-config-go demonstrates Map's
// CompletionConfig alone. The scenario: a batch of orders is priced via
// Map (one Map iteration per order); ONE SPECIFIC order in the batch
// additionally requires polling an external shipping-carrier API via
// WaitForCondition before it's considered fully processed; and the WHOLE
// BATCH's completion requires a human approval callback via
// WaitForCallback before the execution finishes - a genuinely different
// combination and nesting shape from any single existing example.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

// BatchEvent is this example's input shape: a batch of orders to price,
// plus which single order (if any) requires carrier-status polling.
type BatchEvent struct {
	OrderIDs []string `json:"orderIds"`
	// CarrierPollOrderID identifies the ONE order in OrderIDs (if any -
	// empty means none) whose Map iteration additionally polls an
	// external carrier status via operations.WaitForCondition before
	// that iteration's own result is considered final. Modeled as a
	// single discriminant field (rather than, say, a bool-per-order map)
	// since this example's whole point is demonstrating that ONE
	// specific item can carry extra durable-operation complexity beyond
	// what its Map siblings do - not that every item could.
	CarrierPollOrderID string `json:"carrierPollOrderId,omitempty"`
}

// BatchResult is this example's output shape.
type BatchResult struct {
	PricedOrders   []PricedOrder `json:"pricedOrders"`
	ApprovalStatus string        `json:"approvalStatus"`
}

// PricedOrder is one Map iteration's result. CarrierStatus is populated
// ONLY for the one order that went through the WaitForCondition polling
// path (empty string for every other order in the batch).
type PricedOrder struct {
	OrderID       string  `json:"orderId"`
	Price         float64 `json:"price"`
	CarrierStatus string  `json:"carrierStatus,omitempty"`
}

// carrierPollState is checkpointed after each poll - the state
// WaitForCondition threads between checkFn calls, mirroring
// wait-for-condition-go's jobPollState exactly (same shape, different
// name, since this is a genuinely separate operation instance nested
// inside a Map iteration here rather than at the top level).
type carrierPollState struct {
	PollCount int    `json:"pollCount"`
	Status    string `json:"status"`
}

// handler demonstrates three durable operations composed in a single
// workflow, in a genuinely different combination from any other example
// in this repo:
//
//  1. operations.Map prices every order in the batch concurrently, each
//     within its own isolated child context (CONTEXT/MAP +
//     CONTEXT/MAP_ITERATION, per the confirmed real-backend flowchart) -
//     the same fan-out pattern map-parallel-go/completion-config-go use.
//  2. Inside ONE SPECIFIC Map iteration (the order matching
//     event.CarrierPollOrderID), operations.WaitForCondition polls a
//     simulated external shipping-carrier status API until it reports
//     "in_transit" - nested INSIDE a Map iteration's own child context,
//     a genuinely different nesting depth from wait-for-condition-go's
//     top-level-only WaitForCondition call.
//  3. AFTER the entire Map batch completes (every order priced, and the
//     one polling order's carrier status resolved), operations.WaitForCallback
//     gates the WHOLE BATCH's completion on a human approval - modeling a
//     manager who must approve the priced batch as a whole before it is
//     considered final, exactly once, regardless of how many orders were
//     in the batch. This is the key structural difference from
//     wait-for-callback-go's own single-order approval: here the
//     callback scopes the ENTIRE Map's aggregate result, not a single
//     item's own processing.
//
// Each operation checkpoints independently, so a suspend-and-resume (the
// callback suspends the whole execution until approved) only ever
// re-runs whichever work hadn't yet completed - every already-priced
// order and the already-resolved carrier poll all replay-skip via their
// own checkpoints on the second invocation.
func handler(event BatchEvent, dc types.DurableContext) (BatchResult, error) {
	dc.Logger().Info("handler started", map[string]any{"orderCount": len(event.OrderIDs), "carrierPollOrderId": event.CarrierPollOrderID})

	pricedBatch, err := operations.Map(dc, "price-orders", event.OrderIDs,
		func(child types.DurableContext, orderID string, index int) (PricedOrder, error) {
			price, err := operations.Step(child, "price-lookup", func(sc types.StepContext) (float64, error) {
				// In production this would call a real pricing service.
				return 9.99 + float64(len(orderID)), nil
			})
			if err != nil {
				return PricedOrder{}, fmt.Errorf("pricing order %s: %w", orderID, err)
			}

			result := PricedOrder{OrderID: orderID, Price: price}

			// Only the ONE order matching CarrierPollOrderID additionally
			// polls carrier status - every other Map iteration is a plain
			// price lookup, exactly like map-parallel-go's own
			// price-orders Map. This is what makes the combination
			// genuinely distinct: nesting a full WaitForCondition
			// poll-until-terminal loop INSIDE one specific Map iteration's
			// own child context, not at the top level.
			if orderID == event.CarrierPollOrderID {
				final, err := operations.WaitForCondition(child, "poll-carrier-status",
					func(sc types.StepContext, state carrierPollState) (operations.ConditionResult[carrierPollState], error) {
						// In production: call the shipping carrier's real
						// tracking API. Here we simulate a shipment that
						// reports "pending" for its first 2 polls and
						// "in_transit" on the 3rd, mirroring
						// wait-for-condition-go's identical polling
						// cadence so both examples' behavior is easy to
						// compare directly.
						next := carrierPollState{PollCount: state.PollCount + 1}
						if next.PollCount >= 3 {
							next.Status = "in_transit"
						} else {
							next.Status = "pending"
						}
						sc.Logger().Info("polled carrier status", map[string]any{"orderId": orderID, "pollCount": next.PollCount, "status": next.Status})

						return operations.ConditionResult[carrierPollState]{
							State:        next,
							ConditionMet: next.Status == "in_transit",
						}, nil
					},
					carrierPollState{},
					// Poll every 5 seconds (in production), up to 10
					// times - a fixed-delay strategy, matching
					// wait-for-condition-go's own choice for the same
					// reasoning (polling intervals for status checks are
					// typically constant, not escalating).
					operations.WithConditionRetryStrategy[carrierPollState](utils.Presets.FixedDelay(types.Duration{Seconds: 5}, 10)),
				)
				if err != nil {
					return PricedOrder{}, fmt.Errorf("polling carrier status for order %s: %w", orderID, err)
				}
				result.CarrierStatus = final.Status
			}

			return result, nil
		},
	)
	if err != nil {
		return BatchResult{}, fmt.Errorf("pricing orders: %w", err)
	}

	priced := make([]PricedOrder, len(pricedBatch.Items))
	for i, item := range pricedBatch.Items {
		priced[i] = item.Value
	}

	// The WHOLE BATCH's completion is gated on a SINGLE human approval
	// callback, scoped to the aggregate result - not per-order, and not
	// per-Map-iteration. This runs AFTER the Map above fully resolves
	// (every order priced, including the carrier-polling one), so the
	// approver reviews the COMPLETE priced batch, matching a realistic
	// "manager reviews the whole batch before it ships" workflow.
	decision, err := operations.WaitForCallback[string](dc, "batch-approval", func(sc types.StepContext, callbackID string) error {
		// In production: email/notify the approving manager with a
		// summary of the priced batch and an approve/reject link
		// encoding callbackID. Here we just log it, mirroring
		// wait-for-callback-go's identical "log, don't really notify"
		// scope.
		sc.Logger().Info("awaiting batch approval", map[string]any{
			"callbackId": callbackID,
			"orderCount": len(priced),
		})
		return nil
	})
	if err != nil {
		return BatchResult{}, fmt.Errorf("batch approval: %w", err)
	}

	dc.Logger().Info("handler completed", map[string]any{"orderCount": len(priced), "approvalStatus": decision})
	return BatchResult{PricedOrders: priced, ApprovalStatus: decision}, nil
}
