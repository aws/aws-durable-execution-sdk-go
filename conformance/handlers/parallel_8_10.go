// Requirement 8-10: Parallel tolerated-failure-count exceeded (stops
// early).
//
// From test-requirements/parallel/8-10.yaml:
//
//	description: Parallel stops early once the failure count exceeds the
//	  tolerated-failure-count
//	handler: |
//	  Handler invokes the parallel operation with three branches and a
//	  completion config of tolerated-failure-count=1. Max-concurrency is
//	  1 so branches run sequentially: branch 0 fails (failure count 1,
//	  not yet exceeding the tolerance), branch 1 also fails (failure
//	  count 2, now exceeding the tolerance of 1), so the operation stops
//	  early and branch 2 is never started. The handler returns a
//	  projection {completionReason, successCount, failureCount,
//	  totalCount}. Completion reason is FAILURE_TOLERANCE_EXCEEDED.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: FAILURE_TOLERANCE_EXCEEDED
//	    successCount: 0
//	    failureCount: 2
//	    totalCount: 2
//
// # Update: now reads the projection directly off BatchResult's own
// # real fields/methods
//
// operations.Parallel now always returns a valid BatchResult with a nil
// error, even when ToleratedFailureCount is exceeded (see batch.go's own
// completion-policy-contract fix doc) - this handler no longer needs to
// catch a *BatchFailedError and manually reconstruct these counts from
// its embedded *AggregateError. This requirement's own projection omits
// "status" (unlike 8-6/8-9), so the narrower parallelCountsProjection
// struct (shared with 8-13/8-18) is still used.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// parallelCountsProjection is the {completionReason, successCount,
// failureCount, totalCount}-shaped result (no "status" key) several
// fail-fast requirements in this suite return (8-10, 8-13, 8-18).
type parallelCountsProjection struct {
	CompletionReason string `json:"completionReason"`
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
}

func newParallelCountsProjection[T any](batch operations.BatchResult[T]) parallelCountsProjection {
	return parallelCountsProjection{
		CompletionReason: batch.CompletionReason,
		SuccessCount:     batch.SucceededCount(),
		FailureCount:     batch.FailureCount(),
		TotalCount:       batch.TotalCount(),
	}
}

func init() {
	Register("8-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_10Handler, config(client))
	})
}

func parallel8_10Handler(event any, dc types.DurableContext) (parallelCountsProjection, error) {
	tolerated := 1
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "unreached", nil },
	}

	batch, err := operations.Parallel(dc, "tolerated-exceeded", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return parallelCountsProjection{}, err
	}

	return newParallelCountsProjection(batch), nil
}
