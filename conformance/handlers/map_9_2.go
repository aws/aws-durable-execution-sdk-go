// Requirement 9-2: Map items-only form (no operation name).
//
// From test-requirements/map/9-2.yaml:
//
//	description: Map invoked with the items-only form (no name
//	  argument), each item returns directly
//	handler: |
//	  Handler invokes the map operation with the items-only form (no
//	  name argument). The map function returns a value directly for each
//	  item without running an inner step, so each MapIteration child
//	  context has no nested operations. Items are the numbers [1, 2] and
//	  the map function doubles each item. Max-concurrency is set to 1
//	  for a deterministic execution history. The handler returns the
//	  ordered array of successful item results, in item index order.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [2, 4]
//
// operations.Map's Go signature (batch.go) always requires an explicit
// id string - there is no items-only overload the way JS/Python's own
// Map(items, fn) (optional name) does. This is purely a call-shape
// difference, not a checkpointed-behavior one: 9-2's own
// ExpectedExecutionHistory never asserts anything about the parent Map
// operation's Name anywhere (no ContextStartedDetails/
// ContextSucceededDetails field carries it), so passing a fixed,
// arbitrary name string here reproduces every checkpointed event this
// requirement actually verifies - unlike 9-12/9-19/9-20's genuine
// currently-nonexistent-feature gaps, this is fully coverable.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_2Handler, config(client))
	})
}

func map9_2Handler(event any, dc types.DurableContext) ([]int, error) {
	items := []int{1, 2}

	batch, err := operations.Map(dc, "map", items,
		func(child types.DurableContext, item int, index int) (int, error) {
			return item * 2, nil
		},
		operations.WithMapMaxConcurrency[int, int](1),
	)
	if err != nil {
		return nil, err
	}

	results := make([]int, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
