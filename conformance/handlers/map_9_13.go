// Requirement 9-13: Map with a custom item namer.
//
// From test-requirements/map/9-13.yaml:
//
//	description: Map with a custom item namer assigns per-iteration
//	  names while producing correct index-ordered results
//	handler: |
//	  Handler invokes the map operation over the items [1, 2] with a
//	  custom item namer that derives a name for each iteration from the
//	  item and its index (for example "item-1", "item-2"). The map
//	  function returns item * 10 directly for each item. Max-concurrency
//	  is 1 for a deterministic execution history. The custom name
//	  affects observability (the iteration operation name) but not
//	  replay determinism or results, so the history is asserted with
//	  wildcard iteration ids. The handler returns the ordered array of
//	  successful item results, in item index order.
//	Input: [1, 2]
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [10, 20]
//
// PREVIOUSLY declared NotImplemented on the conclusion that this Go
// SDK's operations.Map had no item-namer concept at all. Re-examined per
// explicit user request and found to be a real, buildable feature gap,
// not a permanent limitation: the JS reference SDK has a genuine,
// public, distinct MapConfig.itemNamer field (packages/
// aws-durable-execution-sdk-js/src/types/batch.ts: "Function to generate
// custom names for map items"), wired directly into each iteration's own
// checkpoint Name (handlers/map-handler/map-handler.ts:
// `config?.itemNamer ? config.itemNamer(item, index) : undefined`) -
// simple enough to port with no other change needed. Implemented as
// operations.WithMapItemNamer[TIn, TOut](func(item TIn, index int)
// string) (pkg/durable/operations/batch.go), overriding itemName's own
// "batchName[index]" default purely for observability - replay-skip
// matching still keys off each iteration's own index-based step-ID
// prefix regardless of this option, so nothing about determinism is
// affected. ParallelConfig has no analogous field in the JS reference
// SDK either (branches have no natural "item value" to name from),
// matching this option's own Map-only scope - see WithMapItemNamer's
// own doc for the full writeup.
package handlers

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_13Handler, config(client))
	})
}

func map9_13Handler(items []int, dc types.DurableContext) ([]int, error) {
	batch, err := operations.Map(dc, "named-items", items,
		func(child types.DurableContext, item int, index int) (int, error) {
			return item * 10, nil
		},
		operations.WithMapMaxConcurrency[int, int](1),
		operations.WithMapItemNamer[int, int](func(item int, index int) string {
			return fmt.Sprintf("item-%d", item)
		}),
	)
	if err != nil {
		return nil, err
	}
	return batch.GetResults(), nil
}
