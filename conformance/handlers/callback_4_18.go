// Requirement 4-18: Two callbacks — create both then wait in order.
//
// From test-requirements/callback/4-18.yaml:
//
//	description: Create callback A, create callback B, wait for A, wait
//	  for B
//	handler: |
//	  Handler creates callback A using input[0] as the name, then
//	  creates callback B using input[1] as the name, then waits for A's
//	  result, then waits for B's result, then returns both results.
//	invocations: |
//	  - Handler creates callback A and callback B. The SDK checkpoints
//	    CallbackStarted for A and CallbackStarted for B. The invocation
//	    completes and execution suspends.
//	  - Replay 1: Received callback A success. The SDK replays state,
//	    sees callback B still pending, and the invocation completes.
//	  - Replay 2: Received callback B success. The SDK replays both
//	    callbacks as completed, handler returns both results, and
//	    execution succeeds.
//	CallbackActions: A Delay=3, B Delay=4 (A resolves first)
//	Input: ['${CB_NAME_A}', '${CB_NAME_B}']
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Both CreateCallback calls happen before either is awaited - each
// returns its own independent channel immediately (per CreateCallback's
// own doc: "letting the caller do other processing before awaiting the
// promise"), matching the YAML's CallbackStarted-A-then-CallbackStarted-B
// ordering with NO CallbackSucceeded/InvocationCompleted between them.
// AwaitCallback is then called for A first, then B, matching the
// requirement's own name ("wait for A, wait for B", in that order) - A's
// own Delay=3 (vs B's Delay=4) means A really does resolve first, so
// awaiting in declaration order here happens to also be resolution
// order, but the handler's code does not depend on that: AwaitCallback
// on A blocks/suspends correctly regardless of which one resolves first,
// per callback.go's own awaitCallback/WaitForOperation mechanics.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// callback418Result is this handler's returned output shape - see
// callback417Result's doc for why this shape is chosen purely for the
// handler's own clarity (the YAML asserts no specific Result field).
type callback418Result struct {
	A string `json:"a"`
	B string `json:"b"`
}

func init() {
	Register("4-18", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_18Handler, config(client))
	})
}

func callback4_18Handler(event []string, dc types.DurableContext) (callback418Result, error) {
	resultChA, _, err := operations.CreateCallback[string](dc, event[0])
	if err != nil {
		return callback418Result{}, err
	}
	resultChB, _, err := operations.CreateCallback[string](dc, event[1])
	if err != nil {
		return callback418Result{}, err
	}

	resultA := operations.AwaitCallback(dc, resultChA)
	if resultA.Err != nil {
		return callback418Result{}, resultA.Err
	}
	resultB := operations.AwaitCallback(dc, resultChB)
	if resultB.Err != nil {
		return callback418Result{}, resultB.Err
	}

	return callback418Result{A: resultA.Value, B: resultB.Value}, nil
}
