// Requirement 4-6: Create callback failure (external system reports
// failure).
//
// From test-requirements/callback/4-6.yaml:
//
//	description: External system reports failure via
//	  SendDurableExecutionCallbackFailure handler fails
//	handler: |
//	  Handler creates a callback using the input as the callback name
//	  and blocks on the callback result. The external system reports
//	  failure with ErrorType="RejectedError" and ErrorMessage="not
//	  approved". The handler does not catch the error, so the execution
//	  fails with a CallbackError that wraps the original error.
//	invocations: |
//	  - Handler creates a callback. The SDK checkpoints CallbackStarted.
//	    The invocation completes and execution suspends.
//	  - Replay 1: Received callback failure. The SDK observes
//	    CallbackFailed, raises the error to the handler, and execution
//	    fails.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	  ExpectedExecutionHistory: ... ExecutionFailedDetails.Error.Payload.
//	    ErrorMessage: not approved
//
// Same basic CreateCallback + AwaitCallback shape as 4-1, but the
// external system's CallbackActions send a "failure" operation instead
// of "success" - result.Err is a *operations.CallbackFailedError
// wrapping "not approved" (see callback.go's callbackError, which reads
// CallbackDetails.Error.ErrorMessage verbatim), returned uncaught so the
// whole execution genuinely fails with that message surfacing in
// ExecutionFailedDetails.Error.Payload.ErrorMessage.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("4-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_6Handler, config(client))
	})
}

func callback4_6Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event)
	if err != nil {
		return "", err
	}
	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
