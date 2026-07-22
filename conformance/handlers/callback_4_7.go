// Requirement 4-7: Callback + Step + Wait for callback result (failure).
//
// From test-requirements/callback/4-7.yaml:
//
//	description: createCallback then a step then await callback result —
//	  external system reports failure handler does not catch
//	handler: |
//	  Handler creates a callback using the input as the callback name,
//	  then runs a step, then blocks on the callback result. The external
//	  system reports failure with ErrorType="RejectedError" and
//	  ErrorMessage="not approved". The handler does not catch the error,
//	  so execution fails with CallbackError.
//	invocations: |
//	  - Handler creates a callback, then runs a step. The SDK checkpoints
//	    CallbackStarted and StepStarted/StepSucceeded. The invocation
//	    completes and execution suspends waiting for the callback.
//	  - Replay 1: Received callback failure. The SDK replays the callback
//	    and step, observes CallbackFailed, raises the error to the
//	    handler, and execution fails with CallbackError.
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//	ExpectedExecutionHistory: CallbackStarted(2), StepStarted(3),
//	  StepSucceeded(4), InvocationCompleted(5), CallbackFailed(6),
//	  InvocationCompleted(7), ExecutionFailed(8) - i.e. CreateCallback is
//	  called BEFORE the step, and the step itself runs and succeeds
//	  within the SAME first invocation (both its START and SUCCEED
//	  checkpoints appear before the first InvocationCompleted), only
//	  THEN does the invocation suspend waiting on the callback alone.
//
// CreateCallback returns immediately with a channel (per its own doc:
// "returns immediately... letting the caller do other processing before
// awaiting the promise") - the step runs synchronously right after,
// finishing within this same invocation, and AwaitCallback is only
// called last, which is what makes the callback the sole reason this
// invocation suspends.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("4-7", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(callback4_7Handler, config(client))
	})
}

func callback4_7Handler(event string, dc types.DurableContext) (string, error) {
	resultCh, _, err := operations.CreateCallback[string](dc, event)
	if err != nil {
		return "", err
	}

	if _, err := operations.Step(dc, "step", func(sc types.StepContext) (struct{}, error) {
		return struct{}{}, nil
	}); err != nil {
		return "", err
	}

	result := operations.AwaitCallback(dc, resultCh)
	return result.Value, result.Err
}
