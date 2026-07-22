// Requirement 4-3: Create callback general timeout (no external callback
// sent).
//
// From test-requirements/callback/4-3.yaml:
//
//	description: Create callback with a short general timeout; no
//	  external callback is sent so it times out
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a 5-second general timeout and blocks on the callback result.
//	  No external system responds, so the callback times out and the
//	  execution fails with a CallbackError.
//	invocations: |
//	  - Handler creates a callback with a 5-second timeout. The SDK
//	    checkpoints CallbackStarted with Timeout=5. The invocation
//	    completes and execution suspends.
//	  - Replay 1: Callback timed out. The SDK raises CallbackTimeoutError,
//	    and execution fails with CallbackError.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// Uses operations.WithCallbackTimeout (added this session - see
// callback.go's own doc on that option for the real gap it closes:
// CreateCallback previously never set CallbackOptions.TimeoutSeconds at
// all, so a caller had no way to request this). The handler does not
// catch the resulting error - result.Err is returned directly, letting
// the whole execution fail with a *operations.CallbackFailedError
// (Timeout=true), matching this requirement's ExpectedResult.
// ExecutionStatus: FAILED.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("4-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_3Handler, config(client))
	})
}

func callback4_3Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event, operations.WithCallbackTimeout[string](types.Duration{Seconds: 5}))
	if err != nil {
		return "", err
	}
	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
