// Requirement 8-9: Parallel tolerated-failure-count within tolerance
// (all branches complete).
//
// From test-requirements/parallel/8-9.yaml:
//
//	description: Parallel with a tolerated-failure-count tolerating one
//	  failure completes all branches
//	handler: |
//	  Handler invokes the parallel operation with three branches and a
//	  completion config of tolerated-failure-count=1. Max-concurrency is
//	  1 so branches run sequentially: branch 0 succeeds, branch 1 raises
//	  an error and fails, branch 2 succeeds. Because the failure count
//	  (1) does not exceed the tolerated failure count (1), the operation
//	  does not stop early and all branches complete. The handler returns
//	  a projection {completionReason, status, successCount, failureCount,
//	  totalCount}. Completion reason is ALL_COMPLETED, but status is
//	  FAILED because at least one branch failed.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: ALL_COMPLETED
//	    status: FAILED
//	    successCount: 2
//	    failureCount: 1
//	    totalCount: 3
//
// Unlike 8-6 (tolerance exceeded, Parallel returns a *BatchFailedError),
// here ToleratedFailureCount=1 with exactly 1 failure means
// batchCompletion.overallSucceeded's thresholdExceeded check (failed > 1,
// i.e. 1 > 1) is false, so finishBatch's SUCCESS branch runs and
// Parallel returns a populated, non-error BatchResult - the projection is
// built directly from batch.Items, with "status: FAILED" reflecting that
// at least one item failed even though the BATCH itself (as a Go call)
// succeeded.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_9Handler, config(client))
	})
}

func parallel8_9Handler(event any, dc types.DurableContext) (parallelProjection, error) {
	tolerated := 1
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "b0", nil },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "b2", nil },
	}

	batch, err := operations.Parallel(dc, "tolerated", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return parallelProjection{}, err
	}

	return newParallelProjection(batch), nil
}
