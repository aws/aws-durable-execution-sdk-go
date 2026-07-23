// Requirement 7-6: Wait-for-callback external failure caught
// (recovers).
//
// From test-requirements/wait_for_callback/7-6.yaml:
//
//	description: Wait-for-callback where the external system reports
//	  failure, the handler catches it and returns a fixed recovery
//	  message so the execution succeeds
//	handler: |
//	  Handler runs a single wait_for_callback operation using the input
//	  as the operation name, wrapped in error handling. The submitter
//	  completes and the operation suspends. The external system
//	  completes the callback with a failure. The handler catches the
//	  raised error and returns the fixed string "recovered", so the
//	  execution succeeds.
//	invocations: |
//	  - Fresh invoke: SDK checkpoints ContextStarted (SubType
//	    WaitForCallback), creates the inner callback (CallbackStarted,
//	    ParentId pointing to the operation Id), runs the submitter step
//	    (StepStarted/StepSucceeded, ParentId pointing to the operation
//	    Id), the invocation completes and execution suspends.
//	  - Replay 1: External system completed the callback with a
//	    failure. SDK checkpoints CallbackFailed and ContextFailed,
//	    raises the error which the handler catches; the handler returns
//	    "recovered" and execution succeeds.
//	AsyncInvoke: true
//	Input: ${CB_NAME}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: recovered
//	CallbackActions:
//	  - CallbackName: '*'
//	    Operation: failure
//	    Payload:
//	      ErrorType: RejectedError
//	      ErrorMessage: not approved
//	    Delay: 1
//
// Same external-failure shape as 7-4, but the resulting error is
// observed ("caught") and intentionally not propagated - the handler
// returns the fixed fallback string "recovered" instead, matching the
// try/catch-recovers idiom already established by e.g.
// wait_for_condition_6_8.go's own doc comment. durable.WithDurableExecution
// sees a nil error and marks the execution SUCCEEDED with "recovered" as
// its Result.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("7-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCallback7_6Handler, config(client))
	})
}

func waitForCallback7_6Handler(event string, dc types.DurableContext) (string, error) {
	_, cbErr := operations.WaitForCallback[string](dc, event, func(sc types.StepContext, callbackID string) error {
		return nil
	})
	// "Caught": cbErr is observed here and intentionally not propagated
	// - this is the try/catch the YAML describes.
	_ = cbErr

	return "recovered", nil
}
