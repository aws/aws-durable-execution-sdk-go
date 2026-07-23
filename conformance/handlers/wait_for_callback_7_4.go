// Requirement 7-4: Wait-for-callback external failure (uncaught).
//
// From test-requirements/wait_for_callback/7-4.yaml:
//
//	description: Wait-for-callback where the external system reports
//	  failure and the handler does not catch it, so the execution fails
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name. The submitter completes and the operation
//	  suspends. The external system completes the callback with a
//	  failure (ErrorType="RejectedError", ErrorMessage="not approved").
//	  The handler does not catch the error, so the operation fails and
//	  the execution fails.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted,
//	    ParentId pointing to the operation Id), runs the submitter step
//	    (StepStarted/StepSucceeded, ParentId pointing to the operation
//	    Id), the invocation completes and execution suspends.
//	  - Replay 1: External system completed the callback with a
//	    failure. SDK checkpoints CallbackFailed carrying the error,
//	    raises it to the handler which does not catch it, checkpoints
//	    ContextFailed, and execution fails.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: failure
//	    Payload:
//	      ErrorType: RejectedError
//	      ErrorMessage: not approved
//	    Delay: 1
//
// WaitForCallback itself returns the *operations.CallbackFailedError
// (wrapping "not approved", per callback.go's callbackError) directly to
// the caller - the handler returns it uncaught, so
// durable.WithDurableExecution sees a non-nil error and marks the
// execution FAILED, matching ExpectedResult.ExecutionStatus: FAILED.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_4Handler, config(client))
	})
}

func waitForCallback7_4Handler(event string, dc types.DurableContext) (string, error) {
	return operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	})
}
