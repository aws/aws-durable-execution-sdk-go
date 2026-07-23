// Requirement 8-3: Parallel with named branch objects.
//
// From test-requirements/parallel/8-3.yaml:
//
//	description: Parallel invoked with named-branch objects (a name plus
//	  a function)
//	handler: |
//	  Handler invokes the parallel operation with a name and two branches
//	  expressed as named-branch objects, each pairing a name (branch
//	  names "first" and "second") with a branch function. Each function
//	  returns a constant string directly ("one" then "two").
//	  Max-concurrency is 1 for deterministic ordering. The handler
//	  returns the ordered array of successful branch results, in branch
//	  index order.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [one, two]
//
// The Go SDK's operations.Parallel has no named-branch-object concept
// (branches is simply []func(child) (T, error), with no per-branch name
// parameter distinct from the plain branches-only form exercised by
// 8-2) - each branch's own checkpointed Name is always derived from the
// parent's id plus its index (see batch.go's itemName: "<id>[<index>]"),
// with no way for a caller to override an individual branch's own name.
// This requirement's own ExpectedExecutionHistory does not assert any
// Name field on the ParallelBranch events themselves, only Id/ParentId/
// SubType/Result - so the branch "names" ("first"/"second") in the
// YAML's prose are not independently observable in this requirement's
// own assertions, and a plain branches-only Parallel call (identical
// in effect to 8-2, differing only in the parent's own id and branch
// return values) satisfies it faithfully without needing any per-branch
// naming API this SDK does not have.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("8-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_3Handler, config(client))
	})
}

func parallel8_3Handler(event any, dc types.DurableContext) ([]string, error) {
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) { return "one", nil },
		func(child types.DurableContext) (string, error) { return "two", nil },
	}

	batch, err := operations.Parallel(dc, "named", branches, operations.WithParallelMaxConcurrency[string](1))
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
