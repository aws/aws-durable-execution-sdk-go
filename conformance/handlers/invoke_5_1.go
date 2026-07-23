// Requirement 5-1: Invoke basic (target function succeeds).
//
// From test-requirements/invoke/5-1.yaml:
//
//	description: Invoke basic — invoke an echo target function with input, returns same result
//	handler: |
//	  A handler that invokes an echo target function, passing the event input.
//	  The target function returns whatever it receives. The handler returns the invoke result.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName, event)`, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends waiting for target function.
//	  - Replay 1: Re-invoked because target function completed successfully, SDK checkpoints
//	    ChainedInvokeSucceeded with the echoed result, execution succeeds.
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// The simplest possible operations.Invoke call: a single, unnamed invoke
// of conformance/echo-target (see that package's own main.go doc
// comment) with the handler's raw string input, returning whatever the
// target echoes back unchanged. operations.Invoke itself only ever sends
// a single CHAINED_INVOKE/START checkpoint (see invoke.go's own doc) -
// the terminal ChainedInvokeSucceeded checkpoint this requirement's
// ExpectedExecutionHistory expects is entirely backend-driven once the
// real target function's own invocation completes, exactly like Wait's
// terminal checkpoint.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("5-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_1Handler, config(client))
	})
}

func invoke5_1Handler(event string, dc types.DurableContext) (string, error) {
	return operations.Invoke[string, string](dc, "invoke", echoTargetFunctionARN(), event)
}
