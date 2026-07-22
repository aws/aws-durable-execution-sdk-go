// Requirement 8-16: Parallel where all branches fail (within tolerance,
// ALL_COMPLETED).
//
// From test-requirements/parallel/8-16.yaml:
//
//	description: Parallel where every branch fails but the failure count
//	  is within tolerance, so all branches run and the batch completes
//	  with status FAILED
//	handler: |
//	  Handler invokes the parallel operation with three branches,
//	  max-concurrency 1, and a completion config of
//	  tolerated-failure-count=3, so the tolerance accommodates all three
//	  branches and none is skipped. Every branch raises an error. All
//	  three run to completion; the failure count (3) does not exceed the
//	  tolerance (3), so the completion reason is ALL_COMPLETED, but the
//	  batch status is FAILED because there are no successes. The handler
//	  returns a projection {completionReason, status, successCount,
//	  failureCount, totalCount}.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: ALL_COMPLETED
//	    status: FAILED
//	    successCount: 0
//	    failureCount: 3
//	    totalCount: 3
//
// ToleratedFailureCount=3 with exactly 3 failures: thresholdExceeded
// requires failed > 3 (strictly greater), so 3 > 3 is false and the
// tolerance is never exceeded - every branch runs, and
// batchCompletion.overallSucceeded's own explicit "no CompletionConfig
// fields at all defaults to failed==0" special case does NOT apply here
// (ToleratedFailureCount IS set), so with thresholdExceeded false and no
// MinSuccessful configured, overallSucceeded returns true regardless of
// how many failures occurred - Parallel returns a non-error BatchResult
// whose every item failed.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-16", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_16Handler, config(client))
	})
}

func parallel8_16Handler(event any, dc types.DurableContext) (parallelProjection, error) {
	tolerated := 3
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
	}

	batch, err := operations.Parallel(dc, "all-fail", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return parallelProjection{}, err
	}

	return newParallelProjection(batch), nil
}
