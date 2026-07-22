// Requirement 7-8: Wait-for-callback inside a child context.
//
// From test-requirements/wait_for_callback/7-8.yaml:
//
//	description: Wait-for-callback nested inside a child context — the
//	  operation's ContextStarted/Succeeded carry a ParentId pointing to
//	  the enclosing child context
//	handler: |
//	  Handler runs a child context named "wrapper". Inside that child
//	  context it runs a single wait_for_callback operation using the
//	  input as the operation name. The submitter completes and the
//	  operation suspends. The external system completes the callback
//	  with a success payload; the child context returns the callback
//	  result, which the handler returns.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints the outer ContextStarted (SubType
//	    RunInChildContext). Inside it checkpoints ContextStarted (SubType
//	    WaitForCallback) with ParentId pointing to the outer child
//	    context Id, creates the inner callback (CallbackStarted, ParentId
//	    pointing to the wait_for_callback operation Id), runs the
//	    submitter step (StepStarted/StepSucceeded, ParentId pointing to
//	    the wait_for_callback operation Id), the invocation completes and
//	    execution suspends.
//	  - Replay 1: External system completed the callback. SDK checkpoints
//	    CallbackSucceeded, then ContextSucceeded for the wait_for_callback
//	    operation (ParentId pointing to the outer child context), then
//	    ContextSucceeded for the outer child context, and execution
//	    succeeds.
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
// operations.RunInChildContext("wrapper") wraps a single
// operations.WaitForCallback call - the WaitForCallback's own inner
// RunInChildContext composition (see callback.go's own doc) automatically
// derives its operation ID, and therefore its ParentId, from the enclosing
// "wrapper" child context's own hierarchical namespace (the child
// types.DurableContext passed into the outer RunInChildContext's fn) -
// nothing this handler needs to wire up manually, matching child_3_1.go's
// own established "nothing to wire up manually" pattern for simple
// nesting.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("7-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_8Handler, config(client))
	})
}

func waitForCallback7_8Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "wrapper", func(child types.DurableContext) (string, error) {
		return operations.WaitForCallback[string](child, event, func(sc types.StepContext, callbackID string) error {
			return nil
		})
	})
}
