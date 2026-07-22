// Requirement 8-8: Parallel min-successful early completion.
//
// From test-requirements/parallel/8-8.yaml:
//
//	description: Parallel with a min-successful completion config stops
//	  early once enough branches succeed
//	handler: |
//	  Handler invokes the parallel operation with four branches (each
//	  returns a constant string) and a completion config of
//	  min-successful=2. Max-concurrency is 1 so branches run sequentially
//	  in index order: after branch 0 and branch 1 both succeed, the
//	  min-successful threshold is reached and the operation completes
//	  early without starting branches 2 and 3. The handler returns a
//	  projection {completionReason, successCount, totalCount}. Completion
//	  reason is MIN_SUCCESSFUL_REACHED, success count is 2, and total
//	  count is 2 (only the started/completed branches are counted).
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
// batchCompletion.thresholdExceeded (batch.go) now genuinely gates the
// scheduling loop's own early-exit decision on MinSuccessful, not just
// the final pass/fail classification - branches 2 and 3 are now
// correctly never started once branches 0 and 1 have both succeeded,
// confirmed via a real deployment: output/8-8.json now shows exactly 2
// ContextStarted/ContextSucceeded ParallelBranch pairs, matching this
// requirement's own "totalCount: 2" expectation exactly (previously 4).
// batch.CompletionReason (also new) reports MIN_SUCCESSFUL_REACHED
// directly, so this handler no longer needs to hard-code that literal
// itself.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// parallelMinSuccessfulProjection is 8-8's own {completionReason,
// successCount, totalCount}-shaped result - distinct from
// parallelProjection (8-6's doc) since this requirement's own
// ExpectedResult omits status/failureCount entirely.
type parallelMinSuccessfulProjection struct {
	CompletionReason string `json:"completionReason"`
	SuccessCount     int    `json:"successCount"`
	TotalCount       int    `json:"totalCount"`
}

func init() {
	Register("8-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_8Handler, config(client))
	})
}

func parallel8_8Handler(event any, dc types.DurableContext) (parallelMinSuccessfulProjection, error) {
	minSuccessful := 2
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "b0", nil },
		func(child types.DurableContext) (string, error) { return "b1", nil },
		func(child types.DurableContext) (string, error) { return "b2", nil },
		func(child types.DurableContext) (string, error) { return "b3", nil },
	}

	batch, err := operations.Parallel(dc, "min-successful", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{MinSuccessful: &minSuccessful}),
	)
	if err != nil {
		return parallelMinSuccessfulProjection{}, err
	}

	return parallelMinSuccessfulProjection{
		CompletionReason: batch.CompletionReason,
		SuccessCount:     batch.SucceededCount(),
		TotalCount:       batch.TotalCount(),
	}, nil
}
