// Requirement 4-12: Callback success → Wait → verify replay.
//
// From test-requirements/callback/4-12.yaml:
//
//	description: Callback resolves first, then a wait suspends, then
//	  handler returns — verifies SDK replays callback and wait correctly
//	  across 3 invocations
//	handler: |
//	  Handler creates a callback using the input as the callback name,
//	  blocks on the result, then starts a 2-second wait, then returns
//	  the callback result. This exercises 3 invocations and replay logic
//	  for both callback and wait.
//	invocations: |
//	  - Handler creates a callback and blocks on the callback result. The
//	    SDK checkpoints CallbackStarted. The invocation completes and
//	    execution suspends.
//	  - Replay 1: Received callback success. The SDK replays
//	    CallbackSucceeded, the callback resolves, the handler starts a
//	    2-second wait. The SDK checkpoints WaitStarted. The invocation
//	    completes and execution suspends.
//	  - Replay 2: Wait completed. The SDK replays the callback and wait
//	    as done, the handler returns, and execution succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Unlike 4-9/4-10/4-11 (Wait started BEFORE AwaitCallback, so both are
// in flight concurrently), this requirement fully resolves the callback
// FIRST (AwaitCallback blocks/suspends on its own in the first
// invocation), and only starts the Wait afterward, once the callback's
// value is already in hand - matching the YAML's own EventId ordering
// (CallbackStarted then, only after CallbackSucceeded, WaitStarted).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_12Handler, config(client))
	})
}

func callback4_12Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event)
	if err != nil {
		return "", err
	}

	result := operations.AwaitCallback(dc, resultCh)
	if result.Err != nil {
		return "", result.Err
	}

	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 2}); err != nil {
		return "", err
	}

	return result.Value, nil
}
