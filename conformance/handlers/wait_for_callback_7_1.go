// Requirement 7-1: Wait-for-callback basic (success via external
// callback).
//
// From test-requirements/wait_for_callback/7-1.yaml:
//
//	description: Wait-for-callback basic — submitter registers the
//	  callback id and the external system completes it with a success
//	  payload
//	handler: |
//	  Handler runs a single wait_for_callback operation, using the input
//	  as the operation name. The submitter function receives the
//	  generated callback id and completes without doing anything
//	  durable. The operation then suspends until an external system
//	  completes the callback with a success payload, and that payload
//	  becomes the operation result which the handler returns.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback) for the operation, creates the inner callback
//	    (CallbackStarted with ParentId pointing to the operation Id),
//	    runs the submitter as a step (StepStarted/StepSucceeded with
//	    ParentId pointing to the operation Id), the invocation completes
//	    and execution suspends waiting for the callback.
//	  - Replay 1: External system completed the callback with a success
//	    payload. SDK checkpoints CallbackSucceeded carrying the payload,
//	    then ContextSucceeded for the operation, and execution succeeds.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload: ${CB_PAYLOAD}
//	    Delay: 1
//
// The simplest possible operations.WaitForCallback usage: the submitter
// does nothing durable of its own (it receives the generated callbackID
// but has no need to use it beyond what WaitForCallback's own submitter
// step already checkpoints), and the operation's result is exactly the
// external system's success payload, deserialized as a plain string (the
// default serdes) and returned unchanged. The conformance harness's own
// AsyncInvoke + CallbackActions machinery (see this suite's package-level
// note in wait_for_callback_7_15.go) handles the external
// SendDurableExecutionCallbackSuccess call - this handler only needs to
// call WaitForCallback correctly.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_1Handler, config(client))
	})
}

func waitForCallback7_1Handler(event string, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	})
}
