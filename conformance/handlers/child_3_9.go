// Requirement 3-9: Child context replay (returns cached result).
//
// From test-requirements/child/3-9.yaml:
//
//	description: Child context replay (returns cached result)
//	handler: |
//	  A handler with a child context followed by a wait. On replay after
//	  the wait completes, the child context returns its cached result
//	  without re-executing.
//	  The step returns the input string.
//	  The child context returns the step result.
//	invocations: |
//	  - Handler invokes a child context with a step inside, followed by a
//	    wait. ContextStarted, step executes and succeeds, ContextSucceeded,
//	    WaitStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because wait completed. SDK skips the
//	    completed child context and returns its cached result. WaitSucceeded,
//	    execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// A completed RunInChildContext call followed by a Wait - on the replay
// invocation triggered by the wait elapsing, RunInChildContext's own
// Succeeded replay-skip branch (invoke.go: return
// deserializeContextResult[T](...) without calling fn again) is what
// returns the cached result without re-executing the child's step, no
// different in principle from step_1_9's identical Step-then-Wait
// replay-skip shape, just one level up at the child-context granularity.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_9Handler, config(client))
	})
}

func child3_9Handler(event string, dc types.DurableContext) (string, error) {
	result, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
			return event, nil
		})
	})
	if err != nil {
		return "", err
	}

	if err := operations.Wait(dc, "wait_after_child", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}

	return result, nil
}
