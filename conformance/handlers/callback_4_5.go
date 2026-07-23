// Requirement 4-5: Create callback with heartbeat then success.
//
// From test-requirements/callback/4-5.yaml:
//
//	description: Heartbeat keeps callback alive past initial heartbeat
//	  interval, then a success terminal callback is sent
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  with a 10-second heartbeat timeout and blocks on the callback
//	  result. The external system sends a heartbeat to reset the timer,
//	  then later sends a success callback.
//	invocations: |
//	  - Handler creates a callback with a 10-second heartbeat timeout.
//	    The SDK checkpoints CallbackStarted with HeartbeatTimeout=10. The
//	    invocation completes and execution suspends.
//	  - Replay 1: Received callback success (after heartbeat reset the
//	    timer). The SDK checkpoints CallbackSucceeded with the payload as
//	    Result, and execution succeeds.
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// The heartbeat reset itself (CallbackActions' own "heartbeat" operation,
// sent with no Delay before the later "success" action) is entirely
// external/backend-driven - this handler's own code is identical in
// shape to 4-4 (same WithCallbackHeartbeatTimeout usage, just a longer
// 10s duration matching the YAML), differing only in the external
// system's actions, which this SDK's handler code has no visibility into
// or control over.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_5Handler, config(client))
	})
}

func callback4_5Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event, operations.WithCallbackHeartbeatTimeout[string](types.Duration{Seconds: 10}))
	if err != nil {
		return "", err
	}
	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
