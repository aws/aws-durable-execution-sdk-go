// Requirement 3-13: Child context with wait inside - verify replay.
//
// From test-requirements/child/3-13.yaml:
//
//	description: Child context with wait inside - verify replay
//	handler: |
//	  A child context containing only a wait operation, followed by a
//	  step outside the child.
//	  The child context returns the input string.
//	  The outer step returns the input string.
//	invocations: |
//	  - Handler invokes a child context containing only a wait operation,
//	    followed by a step outside the child. ContextStarted, WaitStarted
//	    with ParentId pointing to child context Id, invocation completes,
//	    execution suspends.
//	  - Replay 1: Re-invoked because wait completed. WaitSucceeded,
//	    ContextSucceeded, outer step executes and succeeds, execution
//	    succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// The child context's fn contains ONLY a Wait call (no nested step at
// all) and returns the input string directly once the wait resolves -
// RunInChildContext's own STARTED/PENDING replay branch re-enters fn on
// the invocation that resumes the wait (see invoke.go's doc: "re-running
// fn from scratch is always safe here"), which re-issues the SAME Wait
// call, replay-skips it (Wait's own Succeeded branch in wait.go), and
// returns immediately. A separate, top-level (outside the child context)
// step then runs and also returns the input string, becoming the whole
// execution's own final result.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("3-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_13Handler, config(client))
	})
}

func child3_13Handler(event string, dc types.DurableContext) (string, error) {
	_, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		if err := operations.Wait(child, "wait", types.Duration{Seconds: 1}); err != nil {
			return "", err
		}
		return event, nil
	})
	if err != nil {
		return "", err
	}

	return operations.Step(dc, "outer_step", func(sc types.StepContext) (string, error) {
		return event, nil
	})
}
