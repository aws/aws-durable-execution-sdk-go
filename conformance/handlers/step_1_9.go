// Requirement 1-9: Replay skips succeeded step.
//
// From test-requirements/step/1-9.yaml:
//
//	description: Replay skips succeeded step
//	handler: |
//	  A step that succeeds, followed by a wait. On replay, the step
//	  returns its checkpointed result without re-executing.
//	invocations: |
//	  - Handler invokes `context.step(func)`, step executes, logs
//	    "step executed", succeeds, wait starts, invocation completes,
//	    execution suspends.
//	  - Replay 1: Re-invoked because wait completed, SDK skips the
//	    completed step (returns cached result without calling the step
//	    function), wait completes, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: cached_value
//	ExpectedExecutionHistory:
//	  ...
//	  WaitStartedDetails:
//	    Duration: 1
//
// Identical structural shape to 1-8 (Step then Wait) but with a distinct
// expected Result ("cached_value") and Wait duration (1s) per this
// requirement's own YAML - kept as a separate file/step id rather than
// reusing 1-8's handler because each requirement must be independently,
// traceably tied to its own conformance scenario (see this package's own
// doc comment), even though the underlying Go logic is structurally
// similar. The step body itself has no side channel to directly observe
// "did fn get called again on replay" from outside the process, so - as
// with 1-8 - conformance relies on the SDK's real replay-skip mechanism
// (runStep's Succeeded branch in step.go) rather than this handler adding
// any extra, non-SDK bookkeeping of its own.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("1-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_9Handler, config(client))
	})
}

func step1_9Handler(event any, dc types.DurableContext) (string, error) {
	result, err := operations.Step(dc, "func", func(sc types.StepContext) (string, error) {
		sc.Logger().Info("step executed", nil)
		return "cached_value", nil
	})
	if err != nil {
		return "", err
	}

	if err := operations.Wait(dc, "wait_after_step", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}

	return result, nil
}
