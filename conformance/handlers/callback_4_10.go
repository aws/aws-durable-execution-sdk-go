// Requirement 4-10: Callback + Wait + Wait for callback result (failure).
//
// From test-requirements/callback/4-10.yaml:
//
//	description: createCallback then 5s wait then await for callback
//	  result — external system reports failure during the wait
//	handler: |
//	  Handler creates a callback using the input as the callback name,
//	  then waits for 5 seconds, then blocks on the callback result. The
//	  external system reports failure during the wait. This is a
//	  3-invocation flow because the callback resolves during the wait.
//	invocations: |
//	  - Handler creates a callback, then starts a 5-second wait, then
//	    blocks on the callback result. The SDK checkpoints CallbackStarted
//	    and WaitStarted (5s). The invocation completes and execution
//	    suspends.
//	  - Replay 1: Received callback failure. The SDK replays state, sees
//	    the wait still pending, and the invocation completes.
//	  - Replay 2: Wait completed. The SDK replays both operations, the
//	    callback raises CallbackError to the handler, and execution
//	    fails.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// Identical Callback-then-Wait-then-AwaitCallback ordering as 4-9;
// differs only in the external system sending a "failure" CallbackAction
// instead of "success" - result.Err is returned uncaught so the
// execution genuinely fails once both the wait and callback have
// resolved.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_10Handler, config(client))
	})
}

func callback4_10Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event)
	if err != nil {
		return "", err
	}

	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 5}); err != nil {
		return "", err
	}

	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
