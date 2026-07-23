// Requirement 4-2: Create callback with explicit name.
//
// From test-requirements/callback/4-2.yaml:
//
//	description: Create callback with an explicit Name
//	handler: |
//	  Handler creates a callback named "approval" and blocks on the
//	  callback result. The external system completes the callback with a
//	  success payload.
//	invocations: |
//	  - Handler creates a callback named "approval". The SDK checkpoints
//	    CallbackStarted with Name="approval". The invocation completes
//	    and execution suspends.
//	  - Replay 1: Received callback success. The SDK checkpoints
//	    CallbackSucceeded with the payload as Result, and execution
//	    succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Identical shape to 4-1, but the callback's id/Name is the literal
// "approval" rather than derived from input (input is unused/nil here -
// event is typed as `any` since the YAML's Input is blank).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_2Handler, config(client))
	})
}

func callback4_2Handler(event any, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, "approval")
	if err != nil {
		return "", err
	}
	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
