// Requirement 8-13: Parallel tolerated-failure-percentage exceeded (stops
// early).
//
// From test-requirements/parallel/8-13.yaml:
//
//	description: Parallel stops early once the failure percentage exceeds
//	  the tolerated-failure-percentage
//	handler: |
//	  Handler invokes the parallel operation with four branches and a
//	  completion config of tolerated-failure-percentage=25. The tolerated
//	  failure percentage is computed against the total branch count (4).
//	  Max-concurrency is 1 so branches run sequentially: branch 0 fails
//	  (1/4 = 25%, not exceeding 25), branch 1 also fails (2/4 = 50%, now
//	  exceeding 25), so the operation stops early and branches 2 and 3
//	  are never started. The handler returns a projection
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
// batchCompletion.thresholdExceeded computes the failure percentage
// against b.total, which is set to len(branches) (4, the FULL configured
// branch count - see Parallel's own `completion := batchCompletion{cfg:
// ..., total: len(branches)}`), not the number of branches that have
// actually started - exactly matching this requirement's own "computed
// against the total branch count (4)" wording: after branch 0 fails,
// 1/4=25% does not exceed 25 (thresholdExceeded requires STRICTLY
// greater-than - see that function's own `pct > *b.cfg.
// ToleratedFailurePercentage`), so branch 1 starts; after branch 1 also
// fails, 2/4=50% > 25 does exceed, stopping branch 2 from ever starting.
//
// # Update: now reads the projection directly off BatchResult's own real
// # fields/methods
//
// operations.Parallel now always returns a valid BatchResult with a nil
// error, even when ToleratedFailurePercentage is exceeded (see batch.go's
// own completion-policy-contract fix doc) - no more catching a
// *BatchFailedError to hand-reconstruct these counts.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_13Handler, config(client))
	})
}

func parallel8_13Handler(event any, dc types.DurableContext) (parallelCountsProjection, error) {
	toleratedPct := 25.0
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "unreached-2", nil },
		func(child types.DurableContext) (string, error) { return "unreached-3", nil },
	}

	batch, err := operations.Parallel(dc, "tolerated-pct", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailurePercentage: &toleratedPct}),
	)
	if err != nil {
		return parallelCountsProjection{}, err
	}

	return newParallelCountsProjection(batch), nil
}
