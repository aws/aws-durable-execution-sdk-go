// Requirement 8-17: Parallel min-successful not reached (all branches
// run, insufficient successes).
//
// From test-requirements/parallel/8-17.yaml:
//
//	description: Parallel with a min-successful threshold that is never
//	  reached runs all branches and completes with ALL_COMPLETED and
//	  status FAILED
//	handler: |
//	  Handler invokes the parallel operation with three branches,
//	  max-concurrency 1, and a completion config of min-successful=3.
//	  Branch 0 succeeds, branch 1 fails, branch 2 succeeds. Only two
//	  branches succeed, so the min-successful threshold (3) is never
//	  reached; there is no failure tolerance configured, so failures do
//	  not stop execution and all three branches run. The completion
//	  reason is ALL_COMPLETED and the status is FAILED (a failure
//	  occurred). The handler returns a projection {completionReason,
//	  status, successCount, failureCount, totalCount}.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: ALL_COMPLETED
//	    status: FAILED
//	    successCount: 2
//	    failureCount: 1
//	    totalCount: 3
//
// # Update: the real semantic mismatch this handler used to document is
// # now FIXED at the SDK level
//
// operations.Parallel now ALWAYS returns a valid, non-error BatchResult
// once its branches finish - including this exact "MinSuccessful not
// reached, no failure tolerance configured, all branches still ran"
// scenario (see batch.go's own completion-policy-contract fix doc). With
// no ToleratedFailureCount/ToleratedFailurePercentage set,
// thresholdExceeded never gates the scheduling loop early on failures
// alone, so all three branches run regardless of MinSuccessful not being
// reached - exactly matching this requirement's own "failures do not
// stop execution and all three branches run" wording. batch.
// CompletionReason correctly resolves to ALL_COMPLETED (not
// MIN_SUCCESSFUL_REACHED, since the threshold was never actually
// reached), and batch.Status()/HasFailure() correctly reflect the one
// genuine per-branch failure - no more catching a *BatchFailedError or
// hand-reconstructing any of these values.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-17", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_17Handler, config(client))
	})
}

func parallel8_17Handler(event any, dc types.DurableContext) (parallelProjection, error) {
	minSuccessful := 3
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "b0", nil },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "b2", nil },
	}

	batch, err := operations.Parallel(dc, "min-not-reached", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{MinSuccessful: &minSuccessful}),
	)
	if err != nil {
		return parallelProjection{}, err
	}

	return newParallelProjection(batch), nil
}
