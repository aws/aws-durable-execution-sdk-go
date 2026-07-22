// Requirement 4-4: Create callback heartbeat timeout (no heartbeat sent).
//
// From test-requirements/callback/4-4.yaml:
//
//	description: Create callback with a short heartbeat timeout; no
//	  heartbeat is sent so it times out
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a 5-second heartbeat timeout and blocks on the callback
//	  result. No heartbeat or terminal callback is sent, so the durable
//	  execution service emits CallbackTimedOut after the heartbeat
//	  interval.
//	invocations: |
//	  - Handler creates a callback with a 5-second heartbeat timeout. The
//	    SDK checkpoints CallbackStarted with HeartbeatTimeout=5. The
//	    invocation completes and execution suspends.
//	  - Replay 1: Heartbeat timed out. The SDK raises CallbackTimeoutError,
//	    and execution fails with CallbackError.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// Uses operations.WithCallbackHeartbeatTimeout (added this session
// alongside WithCallbackTimeout - see callback.go's doc). Same
// error-propagation shape as 4-3: result.Err is returned uncaught so the
// execution genuinely fails.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("4-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_4Handler, config(client))
	})
}

func callback4_4Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event, operations.WithCallbackHeartbeatTimeout[string](types.Duration{Seconds: 5}))
	if err != nil {
		return "", err
	}
	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
