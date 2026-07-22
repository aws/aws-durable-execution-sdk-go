// Requirement 4-8: Callback + Step + Wait for callback result (timeout).
//
// From test-requirements/callback/4-8.yaml:
//
//	description: createCallback (5s timeout) then a step then await
//	  callback result — no callback sent so it times out
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a 5-second timeout, then runs a step, then blocks on the
//	  callback result. No external callback is sent. The handler does
//	  not catch the error, so execution fails with CallbackError after
//	  the timeout fires.
//	invocations: |
//	  - Handler creates a callback with a 5-second timeout, then runs a
//	    step. The SDK checkpoints CallbackStarted (Timeout=5) and
//	    StepStarted/StepSucceeded. The invocation completes and
//	    execution suspends.
//	  - Replay 1: Callback timed out. The SDK replays the callback and
//	    step, observes CallbackTimedOut, raises CallbackTimeoutError to
//	    the handler, and execution fails with CallbackError.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// Same CreateCallback-then-Step-then-AwaitCallback ordering as 4-7, using
// WithCallbackTimeout instead of an external failure action - no
// CallbackActions are sent at all, so the backend's own timeout fires
// after 5s and callbackError (callback.go) surfaces it as a
// *CallbackFailedError with Timeout=true, returned uncaught.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("4-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_8Handler, config(client))
	})
}

func callback4_8Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event, operations.WithCallbackTimeout[string](types.Duration{Seconds: 5}))
	if err != nil {
		return "", err
	}

	if _, err := operations.Step(dc, "step", func(sc types.StepContext) (struct{}, error) {
		return struct{}{}, nil
	}); err != nil {
		return "", err
	}

	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
