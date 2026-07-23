// Requirement 8-18: Parallel with combined completion config
// (min-successful + tolerated-failure-count).
//
// From test-requirements/parallel/8-18.yaml:
//
//	description: Parallel configured with both min-successful and
//	  tolerated-failure-count; the failure tolerance is exceeded first
//	  and stops the operation
//	handler: |
//	  Handler invokes the parallel operation with four branches,
//	  max-concurrency 1, and a completion config combining
//	  min-successful=3 and tolerated-failure-count=1. Branch 0 fails
//	  (failure count 1, within tolerance), branch 1 fails (failure count
//	  2, exceeding the tolerance of 1). The failure tolerance is exceeded
//	  before the min-successful target could be met, so the operation
//	  stops and branches 2 and 3 are never started. The completion reason
//	  is FAILURE_TOLERANCE_EXCEEDED. The handler returns a projection
//	  {completionReason, successCount, failureCount, totalCount}.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: FAILURE_TOLERANCE_EXCEEDED
//	    successCount: 0
//	    failureCount: 2
//	    totalCount: 2
//
// thresholdExceeded checks failure-tolerance BEFORE MinSuccessful (see
// its own doc), so combining both here means the tolerance check alone
// governs early-exit: with ToleratedFailureCount=1, branch 1's failure
// (failed=2 > 1) exceeds it and stops branch 2/3 from ever starting,
// exactly like 8-10's simpler single-threshold scenario, before
// MinSuccessful ever gets a chance to matter.
//
// # Update: now reads the projection directly off BatchResult's own real
// # fields/methods
//
// operations.Parallel now always returns a valid BatchResult with a nil
// error, even when the combined policy is unmet (see batch.go's own
// completion-policy-contract fix doc) - no more catching a
// *BatchFailedError to hand-reconstruct these counts.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("8-18", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_18Handler, config(client))
	})
}

func parallel8_18Handler(event any, dc types.DurableContext) (parallelCountsProjection, error) {
	minSuccessful := 3
	tolerated := 1
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "unreached-2", nil },
		func(child types.DurableContext) (string, error) { return "unreached-3", nil },
	}

	batch, err := operations.Parallel(dc, "combined", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{
			MinSuccessful:         &minSuccessful,
			ToleratedFailureCount: &tolerated,
		}),
	)
	if err != nil {
		return parallelCountsProjection{}, err
	}

	return newParallelCountsProjection(batch), nil
}
