// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring examples/map-virtual-context-go's
// own handler.go/handler_test.go split.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// InventoryCheckEvent is this example's input shape.
type InventoryCheckEvent struct {
	OrderID    string   `json:"orderId"`
	Warehouses []string `json:"warehouses"`
}

// InventoryCheckResult is this example's output shape.
type InventoryCheckResult struct {
	OrderID         string `json:"orderId"`
	TotalUnitsFound int    `json:"totalUnitsFound"`
}

// ParallelVirtualContextHandler demonstrates operations.WithParallelNesting
// (operations.NestingModeFlat) - Parallel's own "virtual context" cost
// optimization, the sibling of map-virtual-context-go's identical
// mechanism one level up (see that example's own doc for the full
// mechanism writeup). Mirrors the JS reference SDK's own
// parallel/virtual-context example ("Demonstrates parallel execution
// with flat nesting for cost optimization").
func ParallelVirtualContextHandler(event InventoryCheckEvent, dc types.DurableContext) (InventoryCheckResult, error) {
	branches := make([]func(types.DurableContext) (int, error), len(event.Warehouses))
	for i, warehouse := range event.Warehouses {
		wh := warehouse
		branches[i] = func(child types.DurableContext) (int, error) {
			return operations.Step(child, "check-inventory", func(sc types.StepContext) (int, error) {
				// A real handler would query the warehouse's own
				// inventory system; this example derives a deterministic
				// placeholder count from the warehouse name's length.
				return len(wh) * 10, nil
			})
		}
	}

	batch, err := operations.Parallel(dc, "check-warehouses", branches,
		operations.WithParallelMaxConcurrency[int](1),
		operations.WithParallelNesting[int](operations.NestingModeFlat),
	)
	if err != nil {
		return InventoryCheckResult{}, fmt.Errorf("order %s: %w", event.OrderID, err)
	}
	if batch.HasFailure() {
		return InventoryCheckResult{}, fmt.Errorf("order %s: %d warehouse checks failed", event.OrderID, batch.FailureCount())
	}

	total := 0
	for _, units := range batch.GetResults() {
		total += units
	}
	return InventoryCheckResult{OrderID: event.OrderID, TotalUnitsFound: total}, nil
}
