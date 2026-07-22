// Requirement 9-4: Map with an empty items list.
//
// From test-requirements/map/9-4.yaml:
//
//	description: Map invoked with an empty items list completes
//	  immediately with no iterations
//	handler: |
//	  Handler invokes the map operation with an empty items list. No
//	  MapIteration child contexts are created; the batch completes
//	  immediately with completion reason ALL_COMPLETED, total count 0,
//	  and an empty results list. The handler returns the ordered array
//	  of successful item results, which is empty.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: []
//
// batch.go's runBatch has an explicit `if n == 0 { return results, true
// }` early return (see map-parallel-go's own SkipVerifications doc for
// the analogous zero-branch Parallel/All case) - Map itself still
// checkpoints its own outer CONTEXT/MAP ContextStarted/ContextSucceeded
// pair (mapID's checkpoint happens before runBatch is ever called), so
// the empty-items case still produces the parent Map context events
// this requirement's own ExpectedExecutionHistory asserts, just with
// zero MapIteration children in between.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("9-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_4Handler, config(client))
	})
}

func map9_4Handler(event any, dc types.DurableContext) ([]string, error) {
	items := []string{}

	batch, err := operations.Map(dc, "empty", items,
		func(child types.DurableContext, item string, index int) (string, error) {
			return item, nil
		},
	)
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
