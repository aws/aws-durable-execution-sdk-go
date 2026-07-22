// Requirement 5-13: Invoke inside child context.
//
// From test-requirements/invoke/5-13.yaml:
//
//	description: Invoke inside child context — invoke within run_in_child_context
//	handler: |
//	  A handler that runs a child context containing an invoke operation.
//	  The invoke succeeds inside the child context, and the child context returns the invoke
//	  result.
//	invocations: |
//	  - Handler invokes `context.runInChildContext((childCtx) => { childCtx.invoke(
//	    targetFunctionName) })`, SDK checkpoints ContextStarted, then ChainedInvokeStarted
//	    with ParentId pointing to child context, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function completed, SDK replays ContextStarted,
//	    checkpoints ChainedInvokeSucceeded, ContextSucceeded, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ... ChainedInvokeStarted ParentId: ${CTX1}
//	  ... ChainedInvokeSucceeded ParentId: ${CTX1}
//	  ... ContextSucceeded Result: '*'
//
// operations.RunInChildContext wrapping a single operations.Invoke call -
// the ChainedInvokeStarted/Succeeded events' own ParentId linkage to the
// child context's own operation ID falls out automatically from
// c.NewChildWithName's hierarchical step-ID namespacing (see invoke.go's
// own RunInChildContext doc, and child_3_6.go's identical precedent for a
// nested operation's ParentId).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("5-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_13Handler, config(client))
	})
}

func invoke5_13Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Invoke[string, string](child, "invoke", echoTargetFunctionARN(), event)
	})
}
