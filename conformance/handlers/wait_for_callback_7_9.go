// Requirement 7-9: Multiple sequential wait-for-callback operations.
//
// From test-requirements/wait_for_callback/7-9.yaml:
//
//	description: Two sequential wait_for_callback operations in one
//	  handler — each is a distinct operation with its own Id, completed
//	  by the external system in order
//	handler: |
//	  Handler runs two wait_for_callback operations back to back. The
//	  first is named "first"; its submitter completes, the operation
//	  suspends, and the external system completes it with a success
//	  payload. The handler then runs the second operation named
//	  "second"; its submitter completes, the operation suspends, and
//	  the external system completes it with a second success payload.
//	  The handler returns the second operation's result.
//	invocations: |
//	  - Fresh invoke: first wait_for_callback checkpoints ContextStarted
//	    (SubType WaitForCallback, Id ${ID1}), creates the inner callback
//	    (CallbackStarted, ParentId ${ID1}), runs the submitter step
//	    (StepStarted/StepSucceeded, ParentId ${ID1}), the invocation
//	    completes and execution suspends.
//	  - Replay 1: External system completed the first callback. SDK
//	    checkpoints CallbackSucceeded and ContextSucceeded for ${ID1}.
//	    The handler then starts the second wait_for_callback in the same
//	    invocation: ContextStarted (Id ${ID2}), CallbackStarted (ParentId
//	    ${ID2}), submitter StepStarted/StepSucceeded (ParentId ${ID2}),
//	    the invocation completes and execution suspends.
//	  - Replay 2: External system completed the second callback. SDK
//	    checkpoints CallbackSucceeded and ContextSucceeded for ${ID2},
//	    and execution succeeds returning the second result.
//	AsyncInvoke: true
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload: ${CB_PAYLOAD1}
//	    Delay: 1
//	  - CallbackName: '*'
//	    Operation: success
//	    Payload: ${CB_PAYLOAD2}
//	    Delay: 1
//
// Two plain, back-to-back WaitForCallback calls with distinct static
// names ("first", "second") - each mints its own operation ID via its own
// NextStepID call, so they are automatically distinct CONTEXT operations
// with no shared state; the second call only runs once the first one's
// own WaitForCallback call has fully returned (Go's normal sequential
// control flow), matching the YAML's own "back to back" / "runs the
// second operation" ordering. The handler returns the SECOND result only,
// per the YAML's own "returns the second operation's result."
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("7-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_9Handler, config(client))
	})
}

func waitForCallback7_9Handler(event any, dc types.DurableContext) (string, error) {
	if _, err := operations.WaitForCallback[string](dc, "first", func(sc types.StepContext, callbackID string) error {
		return nil
	}); err != nil {
		return "", err
	}

	return operations.WaitForCallback[string](dc, "second", func(sc types.StepContext, callbackID string) error {
		return nil
	})
}
