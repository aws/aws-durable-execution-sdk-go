// Requirement 8-21: Nested parallel (a parallel operation inside a
// parallel branch).
//
// From test-requirements/parallel/8-21.yaml:
//
//	description: Parallel whose single branch itself runs a nested
//	  parallel of two steps, verifying nested parallel contexts and
//	  result aggregation
//	handler: |
//	  Handler invokes an outer parallel operation with a single branch
//	  and max-concurrency 1. That branch runs an inner parallel
//	  operation (also max-concurrency 1) with two branches, each a
//	  single step returning a constant string ("i1" then "i2"); the
//	  inner branch returns the inner ordered results ["i1", "i2"]. The
//	  outer parallel therefore produces one result — the inner list — so
//	  the handler returns the outer ordered results, [["i1", "i2"]].
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    - [i1, i2]
//
// operations.Parallel is an ordinary function taking any
// types.DurableContext - including the child context runBatchItem hands
// to a branch's own fn (see batch.go: "child := c.NewChildWithName(...)"
// then "fn(child)") - so calling Parallel again from inside a branch's
// own fn, using that branch's own child context, produces a genuinely
// nested CONTEXT/Parallel operation parented under the outer branch's
// own CONTEXT/ParallelBranch context, with no special support code
// needed: nesting Just Works because every operation in this package
// only ever depends on the types.DurableContext it's given, never on
// being called from the top-level handler specifically.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("8-21", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_21Handler, config(client))
	})
}

func parallel8_21Handler(event any, dc types.DurableContext) ([][]string, error) {
	outerBranches := []func(types.DurableContext) ([]string, error){
		func(branchCtx types.DurableContext) ([]string, error) {
			innerBranches := []func(types.DurableContext) (string, error){
				func(child types.DurableContext) (string, error) {
					return operations.Step(child, "inner-step-1", func(sc types.StepContext) (string, error) {
						return "i1", nil
					})
				},
				func(child types.DurableContext) (string, error) {
					return operations.Step(child, "inner-step-2", func(sc types.StepContext) (string, error) {
						return "i2", nil
					})
				},
			}

			innerBatch, err := operations.Parallel(branchCtx, "inner", innerBranches, operations.WithParallelMaxConcurrency[string](1))
			if err != nil {
				return nil, err
			}

			innerResults := make([]string, len(innerBatch.Items))
			for i, item := range innerBatch.Items {
				innerResults[i] = item.Value
			}
			return innerResults, nil
		},
	}

	outerBatch, err := operations.Parallel(dc, "outer", outerBranches, operations.WithParallelMaxConcurrency[[]string](1))
	if err != nil {
		return nil, err
	}

	outerResults := make([][]string, len(outerBatch.Items))
	for i, item := range outerBatch.Items {
		outerResults[i] = item.Value
	}
	return outerResults, nil
}
