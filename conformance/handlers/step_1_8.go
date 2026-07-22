// Requirement 1-8: Step and wait with replay.
//
// From test-requirements/step/1-8.yaml:
//
//	description: Step and wait with replay
//	handler: |
//	  A step followed by a wait, testing that replay correctly skips the
//	  already-completed step.
//	invocations: |
//	  - Handler invokes `context.step(func)`, step executes and succeeds,
//	    then wait is started, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because wait completed, SDK skips the
//	    completed step, wait checkpoint succeeds, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: computed
//	ExpectedExecutionHistory:
//	  ...
//	  - EventType: WaitStarted
//	    WaitStartedDetails:
//	      Duration: 2
//
// A plain Step followed by operations.Wait for 2 seconds - on the first
// invocation the step runs and checkpoints, the Wait checkpoints and
// suspends the invocation (the backend re-invokes once the 2s timer
// elapses); on replay, the SDK's own replay-skip path (runStep's
// existing.Status == Succeeded branch) returns the step's checkpointed
// result WITHOUT re-running fn, then the Wait's own checkpointed
// Succeeded status lets it return immediately too, and the handler
// returns its final result.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("1-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_8Handler, config(client))
	})
}

func step1_8Handler(event any, dc types.DurableContext) (string, error) {
	result, err := operations.Step(dc, "func", func(sc types.StepContext) (string, error) {
		return "computed", nil
	})
	if err != nil {
		return "", err
	}

	if err := operations.Wait(dc, "wait_after_step", types.Duration{Seconds: 2}); err != nil {
		return "", err
	}

	return result, nil
}
