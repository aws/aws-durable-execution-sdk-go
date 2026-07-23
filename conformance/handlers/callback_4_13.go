// Requirement 4-13: Replay — Callback failure caught → Wait → return.
//
// From test-requirements/callback/4-13.yaml:
//
//	description: Handler catches a callback failure and continues with a
//	  wait, verifying replay across 3 invocations
//	handler: |
//	  Handler creates a callback using the input as the callback name,
//	  blocks on the callback result with error handling, and on failure
//	  stores an error message. Then starts a 2-second wait, then
//	  returns the message. The callback failed and error caught by the
//	  handler.
//	invocations: |
//	  - Handler creates a callback and blocks on the callback result with
//	    error handling. The SDK checkpoints CallbackStarted. The
//	    invocation completes and execution suspends.
//	  - Replay 1: Received callback failure. The SDK replays
//	    CallbackFailed, raises the error but the handler catches it,
//	    then starts a 2-second wait. The SDK checkpoints WaitStarted. The
//	    invocation completes and execution suspends.
//	  - Replay 2: Wait completed. The SDK replays the callback failure
//	    (caught) and wait as done, the handler returns the error
//	    message, and execution succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Unlike 4-6/4-7/4-10 (which return result.Err uncaught, failing the
// whole execution), this handler explicitly checks result.Err, extracts
// its message via Error(), and continues with the Wait + a successful
// return - proving a caught callback failure does not itself terminate
// the execution, exactly matching the ExpectedResult.ExecutionStatus:
// SUCCEEDED.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_13Handler, config(client))
	})
}

func callback4_13Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event)
	if err != nil {
		return "", err
	}

	result := operations.AwaitCallback(dc, resultCh)
	message := result.Value
	if result.Err != nil {
		message = result.Err.Error()
	}

	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 2}); err != nil {
		return "", err
	}

	return message, nil
}
