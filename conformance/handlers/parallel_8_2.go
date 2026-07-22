// Requirement 8-2: Parallel branches-only form (no operation name).
//
// From test-requirements/parallel/8-2.yaml:
//
//	description: Parallel invoked with the branches-only form (no name
//	  argument)
//	handler: |
//	  Handler invokes the parallel operation using the form that omits
//	  the leading operation name, passing two branches. Each branch
//	  returns a constant string directly without an inner step ("alpha"
//	  for branch 0, "beta" for branch 1). Max-concurrency is 1 so
//	  branches run sequentially in index order. The handler returns the
//	  ordered array of successful branch results.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [alpha, beta]
//
// The Go SDK's operations.Parallel has no "branches-only" overload (id is
// always a required positional parameter, unlike the reference SDKs'
// optional-leading-name overload) - the closest faithful equivalent is
// simply passing an empty-string id, which still produces a single,
// unambiguous ContextStarted (SubType Parallel) checkpoint exactly like a
// named call, satisfying this requirement's own ExpectedExecutionHistory
// (which asserts no Name field at all, only Id/ParentId/SubType). Each
// branch returns its constant directly with no inner Step, matching the
// YAML's "without an inner step" wording.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_2Handler, config(client))
	})
}

func parallel8_2Handler(event any, dc types.DurableContext) ([]string, error) {
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "alpha", nil },
		func(child types.DurableContext) (string, error) { return "beta", nil },
	}

	batch, err := operations.Parallel(dc, "", branches, operations.WithParallelMaxConcurrency[string](1))
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
