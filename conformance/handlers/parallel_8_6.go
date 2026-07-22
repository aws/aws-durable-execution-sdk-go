// Requirement 8-6: Parallel fail-fast via tolerated-failure-count=0 stops
// after first failure.
//
// From test-requirements/parallel/8-6.yaml:
//
//	description: Parallel configured with a fail-fast completion config
//	  (tolerated-failure-count=0) stops on the first branch failure and
//	  does not start remaining branches
//	handler: |
//	  Handler invokes the parallel operation with three branches and a
//	  fail-fast completion config (tolerated-failure-count=0).
//	  Max-concurrency is 1 so branches run sequentially: branch 0 returns
//	  "ok" and succeeds, branch 1 raises an error and fails, at which
//	  point fail-fast stops the operation and branch 2 is never started.
//	  The parallel operation itself does not raise; it returns a batch
//	  result. The handler returns a projection object built from the
//	  batch result: {completionReason, status, successCount, failureCount,
//	  totalCount}. Because a failure occurred, status is FAILED and
//	  completion reason is FAILURE_TOLERANCE_EXCEEDED.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    completionReason: FAILURE_TOLERANCE_EXCEEDED
//	    status: FAILED
//	    successCount: 1
//	    failureCount: 1
//	    totalCount: 2
//
// # Update: the real API shape mismatch this handler used to work around
// # is now FIXED at the SDK level
//
// This handler used to hand-roll the YAML's own documented "the parallel
// operation itself does not raise; it returns a batch result" contract
// by catching a *BatchFailedError and manually reconstructing this
// projection from its embedded *AggregateError, because
// operations.Parallel's real behavior at the time returned an error
// (not a BatchResult) whenever the completion policy was unmet.
//
// That mismatch is now fixed directly in operations.Parallel/BatchResult
// (see batch.go's own doc on the completion-policy-contract fix):
// Parallel now ALWAYS returns a valid BatchResult with a nil error once
// its branches finish, exactly matching this requirement's own YAML
// prose - this handler now reads the projection's fields directly off
// BatchResult's own real Status()/CompletionReason/SucceededCount()/
// FailureCount()/TotalCount() methods, with no error-catching/manual
// reconstruction needed at all.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// parallelProjection is the shared {completionReason, status,
// successCount, failureCount, totalCount}-shaped result several
// requirements in this suite (8-6, 8-9, 8-16, 8-17, 8-22) return,
// derived directly from operations.BatchResult's own real fields/
// methods. Fields default-omit via omitempty are deliberately NOT used
// here: every field is always meaningful (including zero counts), and
// the YAML's own ExpectedResult always lists every key explicitly.
type parallelProjection struct {
	CompletionReason string `json:"completionReason"`
	Status           string `json:"status"`
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
}

func newParallelProjection[T any](batch operations.BatchResult[T]) parallelProjection {
	return parallelProjection{
		CompletionReason: batch.CompletionReason,
		Status:           batch.Status(),
		SuccessCount:     batch.SucceededCount(),
		FailureCount:     batch.FailureCount(),
		TotalCount:       batch.TotalCount(),
	}
}

func init() {
	Register("8-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_6Handler, config(client))
	})
}

func parallel8_6Handler(event any, dc types.DurableContext) (parallelProjection, error) {
	tolerated := 0
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "ok", nil },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "unreached", nil },
	}

	batch, err := operations.Parallel(dc, "failfast", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		// A genuine, unrelated error (config validation, suspension,
		// etc.) - Parallel no longer returns an error for a
		// policy-not-met batch (see operations.BatchResult's own doc),
		// so this is unexpected for this scenario and should surface
		// loudly rather than being silently absorbed.
		return parallelProjection{}, err
	}

	return newParallelProjection(batch), nil
}

// errParallelBranchFailed is a plain sentinel used across several of this
// suite's handlers (8-6 through 8-22) whenever a branch's own scenario
// just needs to fail without any specific error content asserted.
var errParallelBranchFailed = errors.New("branch failed")
