// Requirement 4-1: Create callback basic (success via external callback).
//
// From test-requirements/callback/4-1.yaml:
//
//	description: Create callback basic — handler creates a callback and
//	  awaits callback result
//	handler: |
//	  Handler creates a callback using the input as the callback name,
//	  blocks on the callback result, and returns the deserialized result.
//	  The external system completes the callback with a success payload.
//	invocations: |
//	  - Handler creates a callback with the name from input. The SDK
//	    checkpoints CallbackStarted with the generated CallbackId. The
//	    invocation completes and execution suspends.
//	  - Replay 1: Received callback success. The SDK checkpoints
//	    CallbackSucceeded with the payload as Result, and execution
//	    succeeds.
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// The simplest possible CreateCallback usage: create with the input as
// the callback's name, then AwaitCallback (never a raw channel receive -
// see operations.CreateCallback's own doc for why that's a real
// correctness bug, not a style choice) and return the resolved value.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_1Handler, config(client))
	})
}

func callback4_1Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event)
	if err != nil {
		return "", err
	}
	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
