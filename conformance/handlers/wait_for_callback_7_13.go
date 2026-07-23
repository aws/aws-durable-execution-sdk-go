// Requirement 7-13: Wait-for-callback with heartbeat then success.
//
// From test-requirements/wait_for_callback/7-13.yaml:
//
//	description: Wait-for-callback with a heartbeat timeout where the
//	  external system sends a heartbeat to reset the timer, then later
//	  completes the callback with success
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name, configured with a 10-second heartbeat
//	  timeout. The submitter completes and the operation suspends. The
//	  external system first sends a heartbeat (which resets the
//	  heartbeat timer, producing no history event), then completes the
//	  callback with a success payload which becomes the operation
//	  result.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted
//	    with HeartbeatTimeout=10, ParentId pointing to the operation Id),
//	    runs the submitter step (StepStarted/StepSucceeded, ParentId
//	    pointing to the operation Id), the invocation completes and
//	    execution suspends.
//	  - Replay 1: The external system sent a heartbeat (no history
//	    event) then a success callback. SDK checkpoints CallbackSucceeded
//	    with the payload, then ContextSucceeded, and execution succeeds.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: heartbeat
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload: ${CB_PAYLOAD}
//	    Delay: 1
//
// Uses operations.WithWaitForCallbackHeartbeatTimeout(10s), the same new
// option 7-12 exercises for the timeout-firing path - this requirement
// instead confirms the option's heartbeat-timeout wiring does NOT
// spuriously time out the callback when a heartbeat (the harness's own
// CallbackActions Operation: heartbeat, resolved via the real
// SendDurableExecutionCallbackHeartbeat API against the matching
// callback id) is actually sent before the terminal success - the
// heartbeat itself produces no history event (per the YAML's own note;
// it only resets the backend's internal heartbeat-timeout timer), so this
// requirement's own ExpectedExecutionHistory looks identical in shape to
// 7-1's plain success case, just with a longer configured heartbeat
// timeout that the heartbeat action successfully avoids tripping.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_13Handler, config(client))
	})
}

func waitForCallback7_13Handler(event string, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	}, operations.WithWaitForCallbackHeartbeatTimeout[string](types.Duration{Seconds: 10}))
}
