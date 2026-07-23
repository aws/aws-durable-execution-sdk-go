// Requirement 5-5: Invoke target fails (execution fails with InvokeError).
//
// From test-requirements/invoke/5-5.yaml:
//
//	description: Invoke target fails — invoked function throws an error, caller gets
//	  ChainedInvokeFailed
//	handler: |
//	  A handler that invokes a target function which throws an error.
//	  The SDK propagates the error as an InvokeError and the execution fails.
//	invocations: |
//	  - Handler invokes `context.invoke(targetFunctionName)`, SDK checkpoints
//	    ChainedInvokeStarted, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because target function failed, SDK checkpoints
//	    ChainedInvokeFailed, handler throws InvokeError, execution fails.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// The invoke call itself requests a deliberate target-side failure via
// echo-target's reserved control payload (see echo-target/main.go's own
// doc comment for the full design: any request shaped
// {"__echoTargetControl": {"fail": true}} makes that target return a Go
// error instead of echoing, which the real backend reports back as a
// genuine ChainedInvokeFailed). The handler does NOT catch the resulting
// *operations.InvokeFailedError - propagating it unhandled all the way
// out of durable.WithDurableExecution is exactly what turns this
// execution's own terminal status into FAILED, matching this
// requirement's own "handler throws InvokeError, execution fails"
// invocations prose (contrast 5-6, which wraps the identical Invoke call
// in error handling instead).
//
// This handler's own genuine, uncaught invoke failure is exactly the
// shape docs/subagent-briefing.md's session notes flag as possibly
// resurfacing the ALREADY-DOCUMENTED "4-3" NotImplemented gap (empty
// ExecutionFailedDetails.Error.Payload on a real, deployed
// ExecutionFailed history event) - see template.yaml's own "4-3" entry;
// if this requirement's real deployed run shows that exact symptom, it
// is the same already-covered gap, not a new one.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// invoke55TargetControl requests echo-target's deliberate-failure branch
// - see this file's own doc comment and echo-target/main.go's
// echoTargetControl for the full design.
type invoke55TargetControl struct {
	Fail bool `json:"fail"`
}

type invoke55Request struct {
	Control invoke55TargetControl `json:"__echoTargetControl"`
}

func init() {
	Register("5-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_5Handler, config(client))
	})
}

func invoke5_5Handler(_ any, dc types.DurableContext) (any, error) {
	return operations.Invoke[invoke55Request, any](dc, "invoke", echoTargetFunctionARN(), invoke55Request{Control: invoke55TargetControl{Fail: true}})
}
