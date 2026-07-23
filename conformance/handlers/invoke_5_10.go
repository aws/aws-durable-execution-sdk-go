// Requirement 5-10: Invoke replay re-throws (failed invoke error re-thrown
// from cache).
//
// From test-requirements/invoke/5-10.yaml:
//
//	description: Invoke replay re-throws — failed invoke followed by wait; on replay, error
//	  is re-thrown from cache
//	handler: |
//	  A handler that wraps invoke in try/catch, catches the error, then calls wait.
//	  On the second replay (after wait completes), the failed invoke error is re-thrown
//	  from cache and caught again without re-invoking.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName)` in try/catch, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function failed, SDK checkpoints
//	    ChainedInvokeFailed, handler catches error, calls `context.wait(1)`, WaitStarted,
//	    invocation completes, execution suspends.
//	  - Replay 2: Re-invoked because wait completed, SDK replays invoke (re-throws cached
//	    error), handler catches again, WaitSucceeded, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ... WaitStartedDetails.Duration: 1 / WaitSucceededDetails.Duration: 1
//
// Combines 5-6's catch-the-invoke-failure shape with 5-9's
// invoke-then-wait sequencing: the target deliberately fails (same
// echo-target control payload as 5-5/5-6), the handler catches the
// resulting *operations.InvokeFailedError on EVERY replay (invoke.go's
// replay-skip branch re-throws the SAME cached error via invokeError(...)
// for a checkpointed OperationStatusFailed operation, rather than
// re-invoking the target - matching this requirement's own "re-throws
// cached error" prose exactly), then proceeds to Wait(1) and returns a
// fallback value, keeping the overall execution SUCCEEDED.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("5-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_10Handler, config(client))
	})
}

func invoke5_10Handler(_ any, dc types.DurableContext) (invoke56FallbackResult, error) {
	_, err := operations.Invoke[invoke55Request, any](dc, "invoke", echoTargetFunctionARN(), invoke55Request{Control: invoke55TargetControl{Fail: true}})
	if err != nil {
		var invokeErr *operations.InvokeFailedError
		if !errors.As(err, &invokeErr) {
			// Not the expected, catchable invoke failure - see
			// invoke_5_6.go's identical discipline/doc for why this must
			// propagate rather than be swallowed.
			return invoke56FallbackResult{}, err
		}
	}

	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 1}); err != nil {
		return invoke56FallbackResult{}, err
	}

	return invoke56FallbackResult{Fallback: true}, nil
}
