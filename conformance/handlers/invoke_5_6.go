// Requirement 5-6: Invoke target fails, caught (try/catch, execution
// succeeds).
//
// From test-requirements/invoke/5-6.yaml:
//
//	description: Invoke target fails, caught — caller wraps invoke in try/catch, execution
//	  succeeds
//	handler: |
//	  A handler that wraps `context.invoke(...)` in a try/catch. The target function fails,
//	  the InvokeError is caught, and the handler returns a fallback value.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName)` inside try/catch, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function failed, SDK checkpoints
//	    ChainedInvokeFailed, handler catches InvokeError, returns fallback value, execution
//	    succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Same deliberate-target-failure request as 5-5 (see that file's own doc
// comment for the echo-target control-payload mechanism), but here the
// handler's own Go code is the try/catch: it inspects the error
// operations.Invoke returns and, for the expected
// *operations.InvokeFailedError case specifically, swallows it and
// returns a fallback value instead of propagating - which is what keeps
// this execution's own terminal status SUCCEEDED rather than FAILED,
// matching this requirement's "execution succeeds" outcome.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// invoke56FallbackResult is returned when the target's deliberate failure
// is caught - the YAML asserts no specific Result value, only
// ExecutionStatus: SUCCEEDED, so any fixed, recognizable fallback shape
// satisfies the requirement.
type invoke56FallbackResult struct {
	Fallback bool `json:"fallback"`
}

func init() {
	Register("5-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_6Handler, config(client))
	})
}

func invoke5_6Handler(_ any, dc types.DurableContext) (invoke56FallbackResult, error) {
	_, err := operations.Invoke[invoke55Request, any](dc, "invoke", echoTargetFunctionARN(), invoke55Request{Control: invoke55TargetControl{Fail: true}})
	if err == nil {
		return invoke56FallbackResult{}, nil
	}

	var invokeErr *operations.InvokeFailedError
	if !errors.As(err, &invokeErr) {
		// Not the expected, catchable invoke failure (e.g. errSuspended
		// propagating through) - must NOT be swallowed, exactly like
		// examples/chained-invoke-go/handler.go's
		// RetryingInventoryCheckHandler's own identical
		// errors.As(&invokeErr) discipline (see that function's doc for
		// the full rationale on why only a genuine, resolved
		// *InvokeFailedError is safe to catch here).
		return invoke56FallbackResult{}, err
	}

	return invoke56FallbackResult{Fallback: true}, nil
}
