// Requirement 5-9: Invoke replay skips (invoke result cached on replay).
//
// From test-requirements/invoke/5-9.yaml:
//
//	description: Invoke replay skips — invoke followed by a wait; on replay, invoke result
//	  is cached
//	handler: |
//	  A handler that invokes a target function, then calls wait. On the second replay
//	  (after wait completes), the invoke result is returned from cache without re-invoking.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName)`, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK checkpoints
//	    ChainedInvokeSucceeded, handler calls `context.wait(1)`, WaitStarted, invocation
//	    completes, execution suspends.
//	  - Replay 2: Re-invoked because wait completed, SDK replays invoke (returns cached
//	    result without re-invoking), WaitSucceeded, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Nothing new needed from operations.Invoke itself here: its own
// replay-skip branch (invoke.go's `if existing, found :=
// c.ExecManager().GetOperation(stepID); found` case, returning the
// checkpointed result directly for OperationStatusSucceeded without
// re-invoking) is exactly what this requirement's own "SDK replays
// invoke (returns cached result without re-invoking)" prose describes -
// this handler simply calls Invoke once, then Wait once, in the same
// straightforward sequence as callback_4_9.go's analogous
// Callback-then-Wait shape; the multi-replay caching behavior falls out
// of invoke.go's existing logic automatically, exercised for real by the
// conformance runner's own suspend/resume across invocations rather than
// anything this handler's code does explicitly.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("5-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_9Handler, config(client))
	})
}

func invoke5_9Handler(event string, dc types.DurableContext) (string, error) {
	result, err := operations.Invoke[string, string](dc, "invoke", echoTargetFunctionARN(), event)
	if err != nil {
		return "", err
	}
	if err := operations.Wait(dc, "wait", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}
	return result, nil
}
