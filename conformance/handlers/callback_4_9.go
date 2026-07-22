// Requirement 4-9: Callback + Wait + Wait for callback result (success).
//
// From test-requirements/callback/4-9.yaml:
//
//	description: createCallback then a duration wait, then wait for
//	  callback result — wait suspends invocation while callback resolves
//	handler: |
//	  Handler creates a callback using the input as the callback name,
//	  then waits for 5 seconds, then blocks on the callback result. The
//	  callback resolves during the wait period.
//	invocations: |
//	  - Handler creates a callback, then starts a 5-second wait, then
//	    blocks on the callback result. The SDK checkpoints CallbackStarted
//	    and WaitStarted (5s). The invocation completes and execution
//	    suspends.
//	  - Replay 1: Received callback success. The SDK replays state, sees
//	    the wait still pending, and the invocation completes.
//	  - Replay 2: Wait completed. The SDK replays both operations as
//	    completed, returns the callback result, and execution succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// CreateCallback returns immediately (its own doc: "do other processing"
// before awaiting), so operations.Wait can run right after it within the
// SAME invocation - both CallbackStarted and WaitStarted are checkpointed
// before the first suspend, matching the YAML's EventId 2/3 ordering.
// AwaitCallback is called only after Wait returns, so on Replay 1 (the
// callback resolving mid-wait) the wait's own WaitForOperation call is
// what's still blocking - this handler's code doesn't need to do
// anything special for that: operations.Wait itself replay-skips or
// re-blocks correctly depending on the wait's own status, exactly per
// wait.go's existing logic, and AwaitCallback (called after Wait
// returns without suspending) then finds the already-resolved callback
// via CreateCallback's replay-skip branch reflected through resultCh.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("4-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_9Handler, config(client))
	})
}

func callback4_9Handler(event string, dc types.DurableContext) (string, error) {
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
