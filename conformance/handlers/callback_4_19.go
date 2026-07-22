// Requirement 4-19: Two callbacks — create both then wait in reverse
// order.
//
// From test-requirements/callback/4-19.yaml:
//
//	description: Create callback A, create callback B, wait for B, wait
//	  for A
//	handler: |
//	  Handler creates callback A using input[0] as the name, then
//	  creates callback B using input[1] as the name, then waits for B's
//	  result first, then waits for A's result, then returns both
//	  results.
//	invocations: |
//	  - Handler creates callback A and callback B. The SDK checkpoints
//	    CallbackStarted for A and CallbackStarted for B. The invocation
//	    completes and execution suspends.
//	  - Replay 1: Received callback A success. The SDK replays state,
//	    sees callback B still pending, and the invocation completes.
//	  - Replay 2: Received callback B success. The SDK replays both
//	    callbacks as completed, handler returns both results, and
//	    execution succeeds.
//	CallbackActions: B Delay=4 (listed first), A Delay=1 (A actually
//	  resolves first despite being awaited second)
//	Input: ['${CB_NAME_A}', '${CB_NAME_B}']
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// The key case this requirement exercises beyond 4-18: the handler
// AWAITS B first (operations.AwaitCallback(dc, resultChB) called before
// the one for A), but A is the one that actually resolves FIRST
// (Delay=1 vs B's Delay=4) - proving AwaitCallback on B genuinely
// blocks/suspends the whole execution while A's own result sits ready,
// unconsumed, on its own already-resolved buffered channel (resultChA;
// CreateCallback's channel is buffered specifically so "the caller is
// never required to receive before CreateCallback returns" - its own
// doc), until the handler gets around to awaiting it after B resolves.
// This is the same two-independent-channels mechanism as 4-18, ordered
// differently at the await call sites; no different SDK behavior is
// needed; this requirement is a correctness proof of the same primitive.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// callback419Result is this handler's returned output shape - see
// callback417Result's doc for why this shape is chosen purely for the
// handler's own clarity (the YAML asserts no specific Result field).
type callback419Result struct {
	A string `json:"a"`
	B string `json:"b"`
}

func init() {
	Register("4-19", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_19Handler, config(client))
	})
}

func callback4_19Handler(event []string, dc types.DurableContext) (callback419Result, error) {
	resultChA, _, err := operations.CreateCallback[string](dc, event[0])
	if err != nil {
		return callback419Result{}, err
	}
	resultChB, _, err := operations.CreateCallback[string](dc, event[1])
	if err != nil {
		return callback419Result{}, err
	}

	resultB := operations.AwaitCallback(dc, resultChB)
	if resultB.Err != nil {
		return callback419Result{}, resultB.Err
	}
	resultA := operations.AwaitCallback(dc, resultChA)
	if resultA.Err != nil {
		return callback419Result{}, resultA.Err
	}

	return callback419Result{A: resultA.Value, B: resultB.Value}, nil
}
