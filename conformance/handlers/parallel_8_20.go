// Requirement 8-20: Parallel BatchResult accessors
// (succeeded/failed/getErrors/hasFailure).
//
// From test-requirements/parallel/8-20.yaml:
//
//	description: Parallel with a mix of successes and a failure,
//	  exercising the batch-result accessors succeeded(), failed(),
//	  getErrors(), and hasFailure
//	handler: |
//	  Handler invokes the parallel operation with three branches,
//	  max-concurrency 1, and a completion config of
//	  tolerated-failure-count=1 so all branches run. Branch 0 succeeds,
//	  branch 1 fails, branch 2 succeeds. From the batch result the
//	  handler builds a projection using the accessor methods: hasFailure
//	  (the has-failure flag), successCount (the length of the succeeded
//	  list), failureCount (the length of the failed list), and
//	  errorCount (the length of the errors list). The projection is
//	  {hasFailure: true, successCount: 2, failureCount: 1, errorCount: 1}.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    hasFailure: true
//	    successCount: 2
//	    failureCount: 1
//	    errorCount: 1
//
// operations.BatchResult[T] (batch.go) now has real accessor methods
// matching the reference SDKs' own BatchResult interface directly
// (SucceededCount, FailureCount, HasFailure, GetErrors, GetResults,
// ThrowIfError, Status, TotalCount - see BatchResult's own top-level doc
// for the full completion-policy-contract fix that added these) - this
// handler now calls those directly instead of manually iterating
// batch.Items to derive the same values by hand.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// parallelAccessorsProjection is 8-20's own {hasFailure, successCount,
// failureCount, errorCount}-shaped result.
type parallelAccessorsProjection struct {
	HasFailure   bool `json:"hasFailure"`
	SuccessCount int  `json:"successCount"`
	FailureCount int  `json:"failureCount"`
	ErrorCount   int  `json:"errorCount"`
}

func init() {
	Register("8-20", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_20Handler, config(client))
	})
}

func parallel8_20Handler(event any, dc types.DurableContext) (parallelAccessorsProjection, error) {
	tolerated := 1
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "b0", nil },
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "b2", nil },
	}

	batch, err := operations.Parallel(dc, "accessors", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return parallelAccessorsProjection{}, err
	}

	return parallelAccessorsProjection{
		HasFailure:   batch.HasFailure(),
		SuccessCount: batch.SucceededCount(),
		FailureCount: batch.FailureCount(),
		ErrorCount:   len(batch.GetErrors()),
	}, nil
}
