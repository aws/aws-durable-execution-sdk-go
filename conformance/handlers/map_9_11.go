// Requirement 9-11: Map real concurrency (max-concurrency > 1)
// preserves index-ordered results.
//
// From test-requirements/map/9-11.yaml:
//
//	description: Map executing items concurrently returns results in
//	  item index order regardless of completion order
//	handler: |
//	  Handler invokes the map operation over three items that each
//	  return a constant string directly ("r0", "r1", "r2") with a
//	  max-concurrency of 2, so up to two items execute concurrently and
//	  their completion order is not deterministic. The handler returns
//	  the ordered array of successful item results, which the SDK
//	  guarantees to be ordered by item index (not completion order).
//	  Because item completion order is nondeterministic under
//	  concurrency, this requirement only asserts the deterministic
//	  parent ContextStarted event plus the final index-ordered result;
//	  the per-iteration child-context events (whose EventIds depend on
//	  completion order) are intentionally not asserted (the matcher
//	  ignores unlisted events).
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [r0, r1, r2]
//
// batch.go's runBatch pre-claims every item's step ID in index order on
// the single parent goroutine BEFORE spawning any item goroutine (see
// that function's doc, bug #3) and always writes results[i] at index i
// regardless of which goroutine finishes first - so index-ordered
// output is guaranteed by construction even though the underlying
// goroutines race freely (bounded by the maxConcurrency=2 semaphore).
package handlers

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("9-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_11Handler, config(client))
	})
}

func map9_11Handler(event any, dc types.DurableContext) ([]string, error) {
	items := []int{0, 1, 2}

	batch, err := operations.Map(dc, "concurrent", items,
		func(child types.DurableContext, item int, index int) (string, error) {
			return fmt.Sprintf("r%d", item), nil
		},
		operations.WithMapMaxConcurrency[int, string](2),
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
