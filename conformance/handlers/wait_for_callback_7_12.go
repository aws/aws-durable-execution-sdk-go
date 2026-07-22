// Requirement 7-12: Wait-for-callback heartbeat timeout (no heartbeat
// sent).
//
// From test-requirements/wait_for_callback/7-12.yaml:
//
//	description: Wait-for-callback configured with a short heartbeat
//	  timeout; no heartbeat or terminal callback is sent, so it times out
//	  on the heartbeat interval
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name, configured with a 5-second heartbeat
//	  timeout. The submitter completes and the operation suspends. No
//	  heartbeat and no terminal callback are ever sent, so the durable
//	  execution service emits CallbackTimedOut with a heartbeat error
//	  type after the heartbeat interval. The handler does not catch it,
//	  so execution fails.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted
//	    with HeartbeatTimeout=5, ParentId pointing to the operation Id),
//	    runs the submitter step (StepStarted/StepSucceeded, ParentId
//	    pointing to the operation Id), the invocation completes and
//	    execution suspends.
//	  - Replay 1: The heartbeat interval elapses with no heartbeat. SDK
//	    checkpoints CallbackTimedOut (heartbeat error), raises it to the
//	    handler which does not catch it, checkpoints ContextFailed, and
//	    execution fails.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	# No CallbackActions - deliberately: no external system ever sends a
//	# heartbeat or terminal callback.
//	ExpectedExecutionHistory: ...
//	  CallbackStartedDetails.HeartbeatTimeout: 5 ...
//	  CallbackTimedOutDetails.Error.Payload.ErrorType: Callback.Heartbeat
//	  ... ExecutionFailed.
//
// Uses operations.WithWaitForCallbackHeartbeatTimeout(5s) - the new
// option added this session (see its own doc in callback.go for the gap
// it closes: WaitForCallback previously exposed no heartbeat-timeout
// option at all, only the lower-level CreateCallback did). Wired through
// to CreateCallback's own WithCallbackHeartbeatTimeout, which checkpoints
// CallbackOptions.HeartbeatTimeoutSeconds on the inner CALLBACK/CALLBACK
// START - the exact field this requirement's own
// ExpectedExecutionHistory.CallbackStartedDetails.HeartbeatTimeout: 5
// asserts. The handler does not catch the resulting
// *operations.CallbackFailedError (Timeout=true, ErrorType
// "Callback.Heartbeat" per callback.go's callbackError), so it propagates
// uncaught and the execution genuinely fails.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("7-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_12Handler, config(client))
	})
}

func waitForCallback7_12Handler(event string, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	}, operations.WithWaitForCallbackHeartbeatTimeout[string](types.Duration{Seconds: 5}))
}
