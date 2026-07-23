// Requirement 5-12: Invoke then step (invoke result used by subsequent
// step).
//
// From test-requirements/invoke/5-12.yaml:
//
//	description: Invoke then step — invoke succeeds, then step uses invoke result
//	handler: |
//	  A handler that first invokes a target function, then passes the invoke result to a
//	  step.
//	  The invoke succeeds, then the step processes the result and returns it.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName)`, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK replays
//	    ChainedInvokeSucceeded, handler invokes `context.step(fn(invokeResult))`, step
//	    succeeds, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// The mirror image of 5-11: operations.Invoke runs first, and its
// checkpointed result feeds a subsequent operations.Step call. Both
// checkpoints happen within the SAME invocation once the invoke's
// terminal outcome is available (whether that is the first live
// invocation, if the backend resolves ChainedInvoke synchronously enough
// within this Lambda invocation's own duration, or a later replay - the
// handler's own code is identical either way, exactly like every other
// operation in this SDK).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("5-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_12Handler, config(client))
	})
}

func invoke5_12Handler(event string, dc types.DurableContext) (string, error) {
	invokeResult, err := operations.Invoke[string, string](dc, "invoke", echoTargetFunctionARN(), event)
	if err != nil {
		return "", err
	}

	return operations.Step(dc, "process", func(sc types.StepContext) (string, error) {
		return invokeResult, nil
	})
}
