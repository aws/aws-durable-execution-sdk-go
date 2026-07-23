// Requirement 8-22: Parallel tolerated-failure-percentage at the boundary
// (not exceeded, ALL_COMPLETED).
//
// From test-requirements/parallel/8-22.yaml:
//
//	description: Parallel where the failure percentage exactly equals the
//	  tolerated-failure-percentage and therefore does not stop the
//	  operation
//	handler: |
//	  Handler invokes the parallel operation with four branches,
//	  max-concurrency 1, and a completion config of
//	  tolerated-failure-percentage=25. Branch 0 fails (1 of 4 = 25%),
//	  branches 1, 2 and 3 succeed. Because 25% does not exceed the
//	  tolerated 25% (the check is strictly greater-than), the operation
//	  is not stopped and all four branches run. The completion reason is
//	  ALL_COMPLETED and the status is FAILED (a failure occurred). The
//	  handler returns a projection {completionReason, status,
//	  successCount, failureCount, totalCount}.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: ALL_COMPLETED
//	    status: FAILED
//	    successCount: 3
//	    failureCount: 1
//	    totalCount: 4
//
// thresholdExceeded's own percentage check is `pct >
// *b.cfg.ToleratedFailurePercentage` (strictly greater-than, batch.go) -
// with exactly 1/4=25% and a configured tolerance of 25, 25 > 25 is
// false, so the tolerance is never considered exceeded and every branch
// runs to completion regardless of failures. Parallel returns a
// non-error BatchResult (overallSucceeded's tolerance check also never
// trips, and no MinSuccessful is configured), so the projection is built
// directly from batch.Items exactly like 8-9/8-16.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("8-22", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_22Handler, config(client))
	})
}

func parallel8_22Handler(event any, dc types.DurableContext) (parallelProjection, error) {
	toleratedPct := 25.0
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "b1", nil },
		func(child types.DurableContext) (string, error) { return "b2", nil },
		func(child types.DurableContext) (string, error) { return "b3", nil },
	}

	batch, err := operations.Parallel(dc, "pct-boundary", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailurePercentage: &toleratedPct}),
	)
	if err != nil {
		return parallelProjection{}, err
	}

	return newParallelProjection(batch), nil
}
