// Requirement 8-12: Parallel with FLAT nesting (virtual branch contexts).
//
// From test-requirements/parallel/8-12.yaml:
//
//	description: Parallel with FLAT nesting executes branches in virtual
//	  contexts, omitting per-branch ContextStarted/ContextSucceeded
//	  events
//	handler: |
//	  Handler invokes the parallel operation with two branches, each
//	  running a single step that returns a constant string ("fa" then
//	  "fb"), max-concurrency 1, and nesting mode FLAT. With FLAT nesting
//	  the branches use virtual contexts, so no per-branch ParallelBranch
//	  child contexts are checkpointed; each branch's step is checkpointed
//	  directly under the parent Parallel context. The handler returns
//	  the ordered array of successful branch results, in branch index
//	  order.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [fa, fb]
//	ExpectedExecutionHistory:
//	  ... ContextStarted SubType Parallel
//	  ... StepStarted/StepSucceeded (ParentId: the Parallel context)
//	  ... StepStarted/StepSucceeded (ParentId: the Parallel context)
//	  ... ContextSucceeded SubType Parallel
//
// # Update: FIXED - this requirement is now genuinely satisfiable
//
// Previously undeliverable: this Go SDK had no "nesting mode" concept at
// all for operations.Parallel, and ParallelOption[TOut] had exactly
// three options (WithParallelMaxConcurrency, WithParallelCompletionConfig,
// WithParallelSerdes), none of which touched child-context creation.
//
// That conclusion was re-examined per explicit user request and found to
// be a real, buildable feature gap, not a permanent limitation: the JS
// reference SDK has a genuine, public, documented NestingType enum
// (packages/aws-durable-execution-sdk-js/src/types/batch.ts:
// NestingType.NESTED/FLAT) plumbed through into run-in-child-context-
// handler.ts's own confirmed virtualContext mechanism - a real, ~30%
// operation-count reduction feature, not a conformance-suite invention.
// Implemented in this Go SDK as operations.NestingMode/
// WithParallelNesting[TOut](operations.NestingModeFlat) (pkg/durable/
// operations/batch.go), built on a new dcontext.Context.
// NewVirtualChildWithName primitive (pkg/durable/context/dcontext.go)
// that ports the JS mechanism directly: a "virtual" branch context still
// gets its own genuine, unique step-ID namespace (so its own nested
// operations don't collide with sibling branches') but never
// checkpoints its own CONTEXT_STARTED/CONTEXT_SUCCEEDED pair, and every
// operation it checkpoints internally reports the OUTER Parallel
// context's own ID as ParentID directly - see runBatchItem's own updated
// doc for the full mechanics.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("8-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_12Handler, config(client))
	})
}

func parallel8_12Handler(event any, dc types.DurableContext) ([]string, error) {
	batch, err := operations.Parallel(dc, "flat",
		[]func(types.DurableContext) (string, error){
			func(child types.DurableContext) (string, error) {
				return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
					return "fa", nil
				})
			},
			func(child types.DurableContext) (string, error) {
				return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
					return "fb", nil
				})
			},
		},
		operations.WithParallelMaxConcurrency[string](1),
		operations.WithParallelNesting[string](operations.NestingModeFlat),
	)
	if err != nil {
		return nil, err
	}
	return batch.GetResults(), nil
}
