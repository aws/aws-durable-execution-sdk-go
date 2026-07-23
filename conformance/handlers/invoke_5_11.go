// Requirement 5-11: Step then invoke (sequential operations).
//
// From test-requirements/invoke/5-11.yaml:
//
//	description: Step then invoke — step succeeds, then invoke succeeds
//	handler: |
//	  A handler that first executes a step, then invokes a target function with the step
//	  result.
//	  The step succeeds, then the invoke succeeds.
//	invocations: |
//	  - Handler invokes `context.step(fn)`, step succeeds (StepStarted, StepSucceeded), then
//	    invokes `context.invoke(targetFunctionName, stepResult)`, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK replays step (skips
//	    re-execution), checkpoints ChainedInvokeSucceeded, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// A plain operations.Step call (mirroring step_1_1.go's own trivial
// greeting-string shape) whose result feeds directly into a subsequent
// operations.Invoke call - proving the two operation types compose
// sequentially within the same handler exactly like Step-then-Wait/
// Callback-then-Wait already do elsewhere in this suite.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("5-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_11Handler, config(client))
	})
}

func invoke5_11Handler(_ any, dc types.DurableContext) (string, error) {
	stepResult, err := operations.Step(dc, "prepare", func(sc types.StepContext) (string, error) {
		return "invoke-5-11-payload", nil
	})
	if err != nil {
		return "", err
	}

	return operations.Invoke[string, string](dc, "invoke", echoTargetFunctionARN(), stepResult)
}
