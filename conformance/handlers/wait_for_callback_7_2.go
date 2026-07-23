// Requirement 7-2: Wait-for-callback with explicit static name
// ("approval").
//
// From test-requirements/wait_for_callback/7-2.yaml:
//
//	description: Wait-for-callback with an explicit static operation
//	  name ("approval")
//	handler: |
//	  Handler runs a single wait_for_callback operation with the
//	  explicit static name "approval". The submitter completes, the
//	  operation suspends, and the external system completes the
//	  callback with a success payload which becomes the operation
//	  result.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback, Name "approval"), creates the inner callback
//	    (CallbackStarted, ParentId pointing to the operation Id), runs
//	    the submitter step (StepStarted/StepSucceeded, ParentId pointing
//	    to the operation Id), the invocation completes and execution
//	    suspends.
//	  - Replay 1: External system completed the callback. SDK
//	    checkpoints CallbackSucceeded with the payload, then
//	    ContextSucceeded, and execution succeeds.
//	AsyncInvoke: true
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload: ${CB_PAYLOAD}
//	    Delay: 1
//
// Same shape as 7-1, but the operation id passed to WaitForCallback is
// the fixed literal "approval" rather than derived from the input -
// ContextStarted's own Name is exactly this id (see WaitForCallback's
// RunInChildContext composition, which checkpoints Name as the id
// argument), matching this requirement's own
// ExpectedExecutionHistory.Name: approval assertion. Input is unused
// (null per the YAML).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_2Handler, config(client))
	})
}

func waitForCallback7_2Handler(event any, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, "approval", func(sc types.StepContext, callbackID string) error {
		return nil
	})
}
