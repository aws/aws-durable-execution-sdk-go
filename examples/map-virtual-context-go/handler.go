// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/map-parallel-go's
// own handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// PriceCheckEvent is this example's input shape.
type PriceCheckEvent struct {
	OrderID string   `json:"orderId"`
	SKUs    []string `json:"skus"`
}

// PriceCheckResult is this example's output shape.
type PriceCheckResult struct {
	OrderID string    `json:"orderId"`
	Prices  []float64 `json:"prices"`
}

// MapVirtualContextHandler demonstrates operations.WithMapNesting
// (operations.NestingModeFlat) - Map's own "virtual context" cost
// optimization: each iteration's own CONTEXT/MAP_ITERATION
// ContextStarted/ContextSucceeded checkpoint pair is skipped entirely
// (roughly a 30% checkpoint-count reduction for large Maps), with each
// item's own inner Step checkpointed directly under the OUTER Map
// context instead of an intermediate per-item context. Mirrors the JS
// reference SDK's own map/virtual-context example ("Demonstrates map
// operation with flat nesting for cost optimization").
//
// See operations.NestingMode's own doc (pkg/durable/operations/batch.go)
// for the full mechanism writeup - this Go SDK's own implementation of
// this exact feature, confirmed against a real conformance requirement
// (9-12) and the JS reference SDK's own NestingType.FLAT/virtualContext
// source.
func MapVirtualContextHandler(event PriceCheckEvent, dc types.DurableContext) (PriceCheckResult, error) {
	batch, err := operations.Map(dc, "price-lookup", event.SKUs,
		func(child types.DurableContext, sku string, index int) (float64, error) {
			return operations.Step(child, "lookup-price", func(sc types.StepContext) (float64, error) {
				// A real handler would call a pricing service here;
				// this example derives a deterministic placeholder price
				// from the SKU's own length for reproducibility.
				return float64(len(sku)) * 9.99, nil
			})
		},
		operations.WithMapMaxConcurrency[string, float64](1),
		operations.WithMapNesting[string, float64](operations.NestingModeFlat),
	)
	if err != nil {
		return PriceCheckResult{}, fmt.Errorf("order %s: %w", event.OrderID, err)
	}
	if batch.HasFailure() {
		return PriceCheckResult{}, fmt.Errorf("order %s: %d price lookups failed", event.OrderID, batch.FailureCount())
	}

	return PriceCheckResult{OrderID: event.OrderID, Prices: batch.GetResults()}, nil
}
