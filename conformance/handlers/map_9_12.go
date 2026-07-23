// Requirement 9-12: Map with FLAT nesting (virtual iteration contexts).
//
// From test-requirements/map/9-12.yaml:
//
//	description: Map with FLAT nesting executes items in virtual
//	  contexts, omitting per-iteration ContextStarted/ContextSucceeded
//	  events
//	handler: |
//	  Handler invokes the map operation over two items, each running a
//	  single step that returns the item value directly ("fa" then "fb"),
//	  max-concurrency 1, and nesting mode FLAT. With FLAT nesting the
//	  items use virtual contexts, so no per-iteration MapIteration child
//	  contexts are checkpointed; each item's step is checkpointed
//	  directly under the parent Map context. The handler returns the
//	  ordered array of successful item results, in item index order.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    - fa
//	    - fb
//	ExpectedExecutionHistory:
//	  ... ContextStarted SubType Map
//	  ... StepStarted/StepSucceeded (ParentId: the Map context)
//	  ... StepStarted/StepSucceeded (ParentId: the Map context)
//	  ... ContextSucceeded SubType Map
//
// The Map analogue of Parallel8N12 (see parallel_8_12.go's own doc for
// the full feature writeup and root-cause history - this is the exact
// same fix, one level up): operations.WithMapNesting[TIn,
// TOut](operations.NestingModeFlat) is Map's own equivalent of
// WithParallelNesting, built on the identical shared runBatchItem/
// dcontext.Context.NewVirtualChildWithName mechanism (pkg/durable/
// operations/batch.go, pkg/durable/context/dcontext.go) - no
// Map-specific code beyond the option itself was needed, since Map and
// Parallel already share runBatchItem's single implementation for
// their per-item/per-branch lifecycle.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_12Handler, config(client))
	})
}

func map9_12Handler(event any, dc types.DurableContext) ([]string, error) {
	batch, err := operations.Map(dc, "flat", []string{"fa", "fb"},
		func(child types.DurableContext, item string, index int) (string, error) {
			return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
				return item, nil
			})
		},
		operations.WithMapMaxConcurrency[string, string](1),
		operations.WithMapNesting[string, string](operations.NestingModeFlat),
	)
	if err != nil {
		return nil, err
	}
	return batch.GetResults(), nil
}
