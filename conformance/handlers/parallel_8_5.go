// Requirement 8-5: Parallel with an empty branches list.
//
// From test-requirements/parallel/8-5.yaml:
//
//	description: Parallel invoked with an empty branches list completes
//	  immediately with no branches
//	handler: |
//	  Handler invokes the parallel operation with an empty branches list.
//	  No branch child contexts are created; the batch completes
//	  immediately with completion reason ALL_COMPLETED, total count 0,
//	  and an empty results list. The handler returns the ordered array of
//	  successful branch results, which is empty.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: []
//
// batch.go's runBatch returns immediately (results=[], ok=true) when n==0,
// with no step ID claimed and no branch goroutine ever spawned - so an
// empty branches slice produces exactly ContextStarted immediately
// followed by ContextSucceeded for the parent Parallel operation, with no
// ParallelBranch events at all, matching this requirement's own
// ExpectedExecutionHistory.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_5Handler, config(client))
	})
}

func parallel8_5Handler(event any, dc types.DurableContext) ([]string, error) {
	branches := []func(types.DurableContext) (string, error){}

	batch, err := operations.Parallel(dc, "empty", branches)
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
