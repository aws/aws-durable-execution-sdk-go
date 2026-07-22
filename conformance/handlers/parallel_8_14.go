// Requirement 8-14: Parallel replay skips succeeded branches across a
// wait suspension.
//
// From test-requirements/parallel/8-14.yaml:
//
//	description: Parallel where one branch suspends on a wait; on resume
//	  the already-succeeded branch is skipped and the batch result is
//	  reconstructed
//	async: true
//	handler: |
//	  Handler invokes the parallel operation with two branches and
//	  max-concurrency 1 (sequential, deterministic). Branch 0 runs a
//	  single step that returns "b0" and succeeds. Branch 1 starts a
//	  2-second wait and then returns "b1". On the first invocation branch
//	  0 completes and branch 1 suspends on its wait, so the invocation
//	  completes and execution suspends. On replay the wait completes,
//	  branch 1 returns, and the already-succeeded branch 0 is skipped
//	  (its result is reconstructed from the checkpoint, not re-executed).
//	  The handler returns the ordered array of successful branch
//	  results, ["b0", "b1"].
//	AsyncInvoke: true
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: [b0, b1]
//
// With WithParallelMaxConcurrency(1), branch 0 fully completes
// (StepStarted/StepSucceeded, then its own ContextSucceeded) before
// branch 1 is even started (batch.go's runBatch: the concurrency
// semaphore is sized 1, so a single in-flight goroutine at a time), so
// branch 1's Wait suspending the whole execution mid-batch is exactly
// what its own doc describes: runBatch's calling goroutine deregisters
// and blocks on scheduler.waitForCompletion, propagating errSuspended
// once the Wait's own WaitForOperation call reports suspension. On
// replay, runBatchItem's own replay-skip (its GetOperation(itemID) check,
// same as RunInChildContext's) returns branch 0's checkpointed result
// without re-running its fn at all, while branch 1 resumes from its own
// still-STARTED context and its Wait's own already-checkpointed
// WaitStarted, returning "b1" once the wait's checkpointed WaitSucceeded
// is observed.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("8-14", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(parallel8_14Handler, config(client))
	})
}

func parallel8_14Handler(event any, dc types.DurableContext) ([]string, error) {
	branches := []func(types.DurableContext) (string, error){
		func(child types.DurableContext) (string, error) {
			return operations.Step(child, "step-b0", func(sc types.StepContext) (string, error) {
				return "b0", nil
			})
		},
		func(child types.DurableContext) (string, error) {
			if err := operations.Wait(child, "wait-b1", types.Duration{Seconds: 2}); err != nil {
				return "", err
			}
			return "b1", nil
		},
	}

	batch, err := operations.Parallel(dc, "replay", branches, operations.WithParallelMaxConcurrency[string](1))
	if err != nil {
		return nil, err
	}

	results := make([]string, len(batch.Items))
	for i, item := range batch.Items {
		results[i] = item.Value
	}
	return results, nil
}
