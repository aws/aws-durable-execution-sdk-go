// Requirement 8-11: Parallel real concurrency (max-concurrency > 1)
// preserves index-ordered results.
//
// From test-requirements/parallel/8-11.yaml:
//
//	description: Parallel executing branches concurrently returns results
//	  in branch index order regardless of completion order
//	handler: |
//	  Handler invokes the parallel operation with three branches that
//	  each return a constant string directly ("r0", "r1", "r2") and a
//	  max-concurrency of 2, so up to two branches execute concurrently
//	  and their completion order is not deterministic. The handler
//	  returns the ordered array of successful branch results, which the
//	  SDK guarantees to be ordered by branch index (not completion
//	  order). Because branch completion order is nondeterministic under
//	  concurrency, this requirement only asserts the deterministic parent
//	  ContextStarted event plus the final index-ordered result; the
//	  per-branch child-context events (whose EventIds depend on
//	  completion order) are intentionally not asserted (the matcher
//	  ignores unlisted events).
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [r0, r1, r2]
//
// With WithParallelMaxConcurrency(2) and three branches, up to two run at
// once (batch.go's runBatch sizes its semaphore to min(maxConcurrency,
// n)); completion order across goroutines is genuinely nondeterministic,
// but runBatch always writes each branch's ItemResult into results[i] by
// its pre-claimed index (never by completion order - see runBatch's own
// doc, bug #3, for why step IDs and result placement are both
// index-keyed), so the returned slice is always ["r0", "r1", "r2"]
// regardless of which branch's goroutine happens to finish first. This
// requirement's own ExpectedExecutionHistory deliberately asserts only
// the parent's own ContextStarted event, leaving the nondeterministic
// per-branch ordering unasserted.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_11Handler, config(client))
	})
}

func parallel8_11Handler(event any, dc types.DurableContext) ([]string, error) {
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "r0", nil },
		func(child types.DurableContext) (string, error) { return "r1", nil },
		func(child types.DurableContext) (string, error) { return "r2", nil },
	}

	batch, err := operations.Parallel(dc, "concurrent", branches, operations.WithParallelMaxConcurrency[string](2))
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
