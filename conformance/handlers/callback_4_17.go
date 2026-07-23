// Requirement 4-17: Two callbacks — sequential create and wait.
//
// From test-requirements/callback/4-17.yaml:
//
//	description: Create callback A, wait for A, then create callback B,
//	  wait for B
//	handler: |
//	  Handler creates callback A using input[0] as the name, waits for
//	  A's result, then creates callback B using input[1] as the name,
//	  waits for B's result, then returns both results.
//	invocations: |
//	  - Handler creates callback A. The SDK checkpoints CallbackStarted
//	    for A. The invocation completes and execution suspends.
//	  - Replay 1: Received callback A success. The SDK replays
//	    CallbackSucceeded for A, handler creates callback B. The SDK
//	    checkpoints CallbackStarted for B. The invocation completes and
//	    execution suspends.
//	  - Replay 2: Received callback B success. The SDK replays both
//	    callbacks, handler returns both results, and execution succeeds.
//	Input: ['${CB_NAME_A}', '${CB_NAME_B}']
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Fully sequential: CreateCallback+AwaitCallback for A completes (and,
// on the first invocation, suspends the whole execution) BEFORE
// CreateCallback for B is even called - matching the YAML's own EventId
// ordering (CallbackStarted A, CallbackSucceeded A, THEN CallbackStarted
// B, CallbackSucceeded B). Each CreateCallback call gets its own
// independent channel/goroutine per callback.go's own doc.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// callback417Result is this handler's returned output shape - the YAML
// asserts no specific Result field, only ExecutionStatus, so this shape
// is chosen purely for the handler's own clarity, not to match a
// specific expected schema.
type callback417Result struct {
	A string `json:"a"`
	B string `json:"b"`
}

func init() {
	Register("4-17", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_17Handler, config(client))
	})
}

func callback4_17Handler(event []string, dc types.DurableContext) (callback417Result, error) {
	resultChA, _, err := operations.CreateCallback[string](dc, event[0])
	if err != nil {
		return callback417Result{}, err
	}
	resultA := operations.AwaitCallback(dc, resultChA)
	if resultA.Err != nil {
		return callback417Result{}, resultA.Err
	}

	resultChB, _, err := operations.CreateCallback[string](dc, event[1])
	if err != nil {
		return callback417Result{}, err
	}
	resultB := operations.AwaitCallback(dc, resultChB)
	if resultB.Err != nil {
		return callback417Result{}, resultB.Err
	}

	return callback417Result{A: resultA.Value, B: resultB.Value}, nil
}
