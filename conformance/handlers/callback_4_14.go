// Requirement 4-14: Replay — Callback timeout caught → Wait → return.
//
// From test-requirements/callback/4-14.yaml:
//
//	description: Handler catches a callback timeout and continues with a
//	  wait, verifying replay
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a 3-second timeout, blocks on the callback result with error
//	  handling, and on timeout stores an error message. Then starts a
//	  2-second wait, then returns. Execution succeeds because the
//	  handler caught the timeout error.
//	invocations: |
//	  - Handler creates a callback with a 3-second timeout and blocks on
//	    the callback result with error handling. The SDK checkpoints
//	    CallbackStarted (Timeout=3). The invocation completes and
//	    execution suspends.
//	  - Replay 1: Callback timed out. The SDK replays CallbackTimedOut,
//	    raises the error but the handler catches it, then starts a
//	    2-second wait. The SDK checkpoints WaitStarted. The invocation
//	    completes and execution suspends.
//	  - Replay 2: Wait completed. The SDK replays the callback timeout
//	    (caught) and wait as done, the handler returns, and execution
//	    succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Same "catch, don't propagate" shape as 4-13, but with
// WithCallbackTimeout(3s) and no CallbackActions at all (letting the
// backend's own timeout fire) instead of an explicit failure action -
// this is the requirement that most directly exercises
// *operations.CallbackFailedError.Timeout being correctly set to true
// (see callback.go's callbackError doc for the bug this session fixed:
// Timeout used to be hard-coded false, and TimedOut used to fall through
// to the success path entirely rather than reaching callbackError at
// all). The YAML's ExpectedResult has no Result field, so this handler
// returns nil rather than the caught message, matching 4-14's own
// "execution succeeds because the handler caught the timeout error" -
// nothing about what value the handler returns afterward is asserted.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-14", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_14Handler, config(client))
	})
}

func callback4_14Handler(event string, dc types.DurableContext) (*string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event, operations.WithCallbackTimeout[string](types.Duration{Seconds: 3}))
	if err != nil {
		return nil, err
	}

	result := operations.AwaitCallback(dc, resultCh)
	_ = result.Err // caught: deliberately not propagated, matching this requirement's "handler catches it" scenario.

	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 2}); err != nil {
		return nil, err
	}

	return nil, nil
}
