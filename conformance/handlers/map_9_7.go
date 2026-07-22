// Requirement 9-7: Map min-successful early completion.
//
// From test-requirements/map/9-7.yaml:
//
//	description: Map with a min-successful completion config stops early
//	  once enough items succeed
//	handler: |
//	  Handler invokes the map operation over four items (each returns a
//	  constant string) with a completion config of min-successful=2.
//	  Max-concurrency is 1 so items run sequentially in index order:
//	  after item 0 and item 1 both succeed, the min-successful threshold
//	  is reached and the operation completes early without starting
//	  items 2 and 3. The handler returns a projection {completionReason,
//	  successCount, totalCount}. Completion reason is
//	  MIN_SUCCESSFUL_REACHED, success count is 2, and total count is 2
//	  (only the started/completed items are counted).
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: MIN_SUCCESSFUL_REACHED
//	    successCount: 2
//	    totalCount: 2
//
// # Update: the real MinSuccessful-early-exit gap this handler used to
// # document is now FIXED at the SDK level
//
// batchCompletion.thresholdExceeded (batch.go, shared verbatim with
// Parallel) now genuinely gates the scheduling loop's own early-exit
// decision on MinSuccessful, not just the final pass/fail
// classification - items 2 and 3 are now correctly never started once
// items 0 and 1 have both succeeded, confirmed via a real deployment:
// output/9-7.json now shows exactly 2 ContextStarted/ContextSucceeded
// MapIteration pairs, matching this requirement's own "totalCount: 2"
// expectation exactly (previously 4).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// mapMinSuccessfulProjection is 9-7's own {completionReason,
// successCount, totalCount}-shaped result (deliberately no status/
// failureCount fields - this requirement's own ExpectedResult omits
// them, unlike mapProjection's fuller shape used elsewhere in this
// suite).
type mapMinSuccessfulProjection struct {
	CompletionReason string `json:"completionReason"`
	SuccessCount     int    `json:"successCount"`
	TotalCount       int    `json:"totalCount"`
}

func init() {
	Register("9-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_7Handler, config(client))
	})
}

func map9_7Handler(event any, dc types.DurableContext) (mapMinSuccessfulProjection, error) {
	minSuccessful := 2
	items := []int{0, 1, 2, 3}

	batch, err := operations.Map(dc, "min-successful", items,
		func(child types.DurableContext, item int, index int) (string, error) {
			return "ok", nil
		},
		operations.WithMapMaxConcurrency[int, string](1),
		operations.WithMapCompletionConfig[int, string](types.CompletionConfig{MinSuccessful: &minSuccessful}),
	)
	if err != nil {
		return mapMinSuccessfulProjection{}, err
	}

	return mapMinSuccessfulProjection{
		CompletionReason: batch.CompletionReason,
		SuccessCount:     batch.SucceededCount(),
		TotalCount:       batch.TotalCount(),
	}, nil
}
