// Requirement 8-1: Parallel basic (two branches, each a single step, all
// succeed).
//
// From test-requirements/parallel/8-1.yaml:
//
//	description: Parallel basic (two branches, each a single step, all
//	  succeed)
//	handler: |
//	  Handler invokes the parallel operation with a fixed name "parallel"
//	  and two branches. Each branch runs a single step that returns a
//	  constant string ("task-1" for branch 0, "task-2" for branch 1).
//	  Max-concurrency is set to 1 so branches execute sequentially in
//	  index order, producing a deterministic execution history. The
//	  handler returns the ordered array of successful branch results, in
//	  branch index order.
//	invocations: |
//	  - Handler invokes parallel(name="parallel", branches=[branch0,
//	    branch1], max-concurrency=1). The SDK checkpoints ContextStarted
//	    (SubType Parallel) for the parent operation. For branch 0 it
//	    checkpoints ContextStarted (SubType ParallelBranch) with ParentId
//	    pointing to the parent, runs the inner step (StepStarted/
//	    StepSucceeded), and checkpoints ContextSucceeded for the branch.
//	    It repeats for branch 1, then checkpoints ContextSucceeded
//	    (SubType Parallel) for the parent carrying the batch summary. The
//	    invocation completes returning ["task-1", "task-2"].
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [task-1, task-2]
//
// operations.WithParallelMaxConcurrency(1) forces branches to run
// strictly sequentially in index order (see batch.go's runBatch: the
// concurrency semaphore is sized 1, and step IDs are pre-claimed in
// index order on the parent goroutine before any branch goroutine is
// spawned), giving a fully deterministic ExecutionHistory ordering to
// assert against - each branch runs a Step (SubType Step) inside its own
// ParallelBranch child context.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_1Handler, config(client))
	})
}

func parallel8_1Handler(event any, dc types.DurableContext) ([]string, error) {
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) {
			return operations.Step(child, "step-1", func(sc types.StepContext) (string, error) {
				return "task-1", nil
			})
		},
		func(child types.DurableContext) (string, error) {
			return operations.Step(child, "step-2", func(sc types.StepContext) (string, error) {
				return "task-2", nil
			})
		},
	}

	batch, err := operations.Parallel(dc, "parallel", branches, operations.WithParallelMaxConcurrency[string](1))
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
