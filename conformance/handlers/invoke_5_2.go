// Requirement 5-2: Invoke with name (explicit name parameter).
//
// From test-requirements/invoke/5-2.yaml:
//
//	description: Invoke with name — invoke echo target with name set from input, assert
//	  name in history
//	handler: |
//	  A handler that invokes an echo target function with a name parameter derived from the input.
//	  The name appears in the ChainedInvokeStarted/Succeeded events.
//	invocations: |
//	  - Handler invokes `context.invoke(event.name, targetFunctionName, event.payload)`, SDK
//	    checkpoints ChainedInvokeStarted with Name matching the input name, invocation completes,
//	    execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK checkpoints
//	    ChainedInvokeSucceeded, execution succeeds.
//	Input:
//	  name: ${NAME_1}
//	  payload: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ... ChainedInvokeStarted Name: ${NAME_1}
//	  ... ChainedInvokeSucceeded Name: ${NAME_1}
//
// operations.Invoke's own "id" parameter IS this SDK's Name (see
// invoke.go: `Name: id` on the checkpointed OperationUpdate) - exactly
// the same relationship Step's "id" has to StepStarted's own Name field
// (see step_1_1.go's precedent). So this requirement's "name parameter
// derived from the input" is satisfied simply by passing event.Name
// itself as operations.Invoke's id argument, rather than a fixed literal
// like every other requirement in this suite uses.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

type invoke52Event struct {
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

func init() {
	Register("5-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_2Handler, config(client))
	})
}

func invoke5_2Handler(event invoke52Event, dc types.DurableContext) (string, error) {
	return operations.Invoke[string, string](dc, event.Name, echoTargetFunctionARN(), event.Payload)
}
