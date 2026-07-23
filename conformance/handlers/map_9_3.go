// Requirement 9-3: Map function receives item and index.
//
// From test-requirements/map/9-3.yaml:
//
//	description: Map function receives both the item and its zero-based
//	  index and uses both to compute the result
//	handler: |
//	  Handler invokes the map operation over the items [10, 20, 30]. The
//	  map function receives each item together with its zero-based index
//	  and returns item + index (10+0=10, 20+1=21, 30+2=32), returning the
//	  value directly without an inner step. Max-concurrency is set to 1
//	  for a deterministic execution history. The handler returns the
//	  ordered array of successful item results, in item index order.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [10, 21, 32]
//
// operations.Map's fn signature already carries index as its third
// parameter (see batch.go: `fn func(child types.DurableContext, item
// TIn, index int) (TOut, error)`), passed through unchanged from Map's
// own `items[i]`/`i` at the call site inside its runBatch closure - no
// extra plumbing needed here beyond using that parameter directly.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("9-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_3Handler, config(client))
	})
}

func map9_3Handler(event any, dc types.DurableContext) ([]int, error) {
	items := []int{10, 20, 30}

	batch, err := operations.Map(dc, "indexed", items,
		func(child types.DurableContext, item int, index int) (int, error) {
			return item + index, nil
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
