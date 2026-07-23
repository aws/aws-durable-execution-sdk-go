// Requirement 9-9: Map tolerated-failure-count exceeded (stops early).
//
// From test-requirements/map/9-9.yaml:
//
//	description: Map stops early once the failure count exceeds the
//	  tolerated-failure-count
//	handler: |
//	  Handler invokes the map operation over three items with a
//	  completion config of tolerated-failure-count=1. Max-concurrency is
//	  1 so items run sequentially: item 0 fails (failure count 1, not
//	  yet exceeding the tolerance), item 1 also fails (failure count 2,
//	  now exceeding the tolerance of 1), so the operation stops early
//	  and item 2 is never started. The handler returns a projection
//	  {completionReason, successCount, failureCount, totalCount}.
//	  Completion reason is FAILURE_TOLERANCE_EXCEEDED.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: FAILURE_TOLERANCE_EXCEEDED
//	    successCount: 0
//	    failureCount: 2
//	    totalCount: 2
//
// # Update: now reads the projection directly off BatchResult's own real
// # fields/methods
//
// operations.Map now always returns a valid BatchResult with a nil
// error, even when ToleratedFailureCount is exceeded (see batch.go's own
// completion-policy-contract fix doc) - no more catching a
// *BatchFailedError to hand-reconstruct these counts.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// mapToleranceProjection is 9-9/9-10's own {completionReason,
// successCount, failureCount, totalCount}-shaped result (no status
// field - this pair's own ExpectedResult omits it, unlike mapProjection
// used elsewhere in this suite).
type mapToleranceProjection struct {
	CompletionReason string `json:"completionReason"`
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
}

func newMapToleranceProjection[T any](batch operations.BatchResult[T]) mapToleranceProjection {
	return mapToleranceProjection{
		CompletionReason: batch.CompletionReason,
		SuccessCount:     batch.SucceededCount(),
		FailureCount:     batch.FailureCount(),
		TotalCount:       batch.TotalCount(),
	}
}

func init() {
	Register("9-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(map9_9Handler, config(client))
	})
}

func map9_9Handler(event any, dc types.DurableContext) (mapToleranceProjection, error) {
	tolerated := 1
	items := []int{0, 1, 2}

	batch, err := operations.Map(dc, "tolerated-exceeded", items,
		func(child types.DurableContext, item int, index int) (string, error) {
			if item == 0 || item == 1 {
				return "", errMapItemFailed
			}
			return "unreached", nil
		},
		operations.WithMapMaxConcurrency[int, string](1),
		operations.WithMapCompletionConfig[int, string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return mapToleranceProjection{}, err
	}

	return newMapToleranceProjection(batch), nil
}
