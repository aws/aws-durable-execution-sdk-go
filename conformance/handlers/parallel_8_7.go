// Requirement 8-7: Parallel throw-if-error propagates a branch failure to
// the execution.
//
// From test-requirements/parallel/8-7.yaml:
//
//	description: Parallel where the handler asks the batch result to
//	  rethrow, propagating a branch failure so the execution fails
//	handler: |
//	  Handler invokes the parallel operation with two branches and a
//	  fail-fast completion config (tolerated-failure-count=0).
//	  Max-concurrency is 1: branch 0 raises an error and fails, so
//	  fail-fast stops the operation and branch 1 is never started. The
//	  parallel operation returns a batch result; the handler then asks
//	  the batch result to rethrow, which rethrows the first branch
//	  failure. The error is not caught, so the execution fails.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// # Update: now uses the real BatchResult.ThrowIfError() method directly
//
// operations.Parallel now always returns a valid BatchResult with a nil
// error, even when the completion policy is unmet (see batch.go's own
// completion-policy-contract fix doc) - this handler now calls the
// batch result's own ThrowIfError() method directly, exactly matching
// the reference SDKs' own "catch the batch result, call rethrow(), let
// it propagate uncaught" idiom this requirement's YAML describes, rather
// than relying on Parallel itself to have already raised the error.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_7Handler, config(client))
	})
}

func parallel8_7Handler(event any, dc types.DurableContext) (*string, error) {
	tolerated := 0
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "", errParallelBranchFailed },
		func(child types.DurableContext) (string, error) { return "unreached", nil },
	}

	batch, err := operations.Parallel(dc, "throwing", branches,
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelCompletionConfig[string](types.CompletionConfig{ToleratedFailureCount: &tolerated}),
	)
	if err != nil {
		return nil, err
	}
	if throwErr := batch.ThrowIfError(); throwErr != nil {
		return nil, throwErr
	}
	return nil, nil
}
