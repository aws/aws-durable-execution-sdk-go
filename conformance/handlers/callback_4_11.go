// Requirement 4-11: Callback + Wait + Wait for callback result (timeout).
//
// From test-requirements/callback/4-11.yaml:
//
//	description: createCallback (3s timeout) then 6s wait then await —
//	  callback times out during the wait
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a 3-second timeout, then waits for 6 seconds, then blocks on
//	  the callback result. No external callback is sent. The callback
//	  times out at t=3s while the lambda is still suspended for the
//	  wait, then the wait completes at t=6s, then the handler
//	  re-invokes and the callback raises CallbackTimeoutError.
//	invocations: |
//	  - Handler creates a callback with a 3-second timeout, then starts a
//	    6-second wait, then blocks on the callback result. The SDK
//	    checkpoints CallbackStarted (Timeout=3) and WaitStarted (6s). The
//	    invocation completes and execution suspends.
//	  - Replay 1: Callback timed out at t=3s. The SDK replays state, sees
//	    the wait still pending, and the invocation completes.
//	  - Replay 2: Wait completed at t=6s. The SDK replays both
//	    operations, the callback raises CallbackTimeoutError to the
//	    handler, and execution fails with CallbackError.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// Same Callback-then-Wait-then-AwaitCallback ordering as 4-9/4-10, using
// WithCallbackTimeout(3s) with a longer 6-second wait so the callback's
// own backend-driven timeout fires strictly before the wait elapses -
// exactly the "callback times out during the wait" ordering the YAML's
// own timestamps (t=3s callback timeout, t=6s wait completion) describe.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("4-11", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_11Handler, config(client))
	})
}

func callback4_11Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event, operations.WithCallbackTimeout[string](types.Duration{Seconds: 3}))
	if err != nil {
		return "", err
	}

	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 6}); err != nil {
		return "", err
	}

	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
