// Requirement 3-10: Child context with step and wait inside.
//
// From test-requirements/child/3-10.yaml:
//
//	description: Child context with step and wait inside
//	handler: |
//	  A child context containing a step followed by a wait operation.
//	  Validates that mixed operation types work correctly within a child
//	  context.
//	  The step returns the input string.
//	  The child context returns the input string.
//	invocations: |
//	  - Handler invokes a child context containing a step followed by a
//	    wait. ContextStarted, StepStarted, StepSucceeded, WaitStarted,
//	    invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because wait completed. WaitSucceeded,
//	    ContextSucceeded, execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// Both the Step AND the Wait are nested INSIDE the child context here
// (unlike 3-9, where the Wait is a top-level sibling of the child
// context) - the Wait call's suspend/resume happens from within fn
// itself, so RunInChildContext's own STARTED/PENDING replay branch (see
// invoke.go: "an interrupted attempt... re-running fn from scratch is
// always safe here because fn's own nested operations are independently
// replay-safe") is what re-enters fn on replay, re-issues the SAME step
// and wait calls in the same order, replay-skips the already-succeeded
// step, and picks the wait back up.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_10Handler, config(client))
	})
}

func child3_10Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		if _, err := operations.Step(child, "step", func(sc types.StepContext) (string, error) {
			return event, nil
		}); err != nil {
			return "", err
		}
		if err := operations.Wait(child, "wait", types.Duration{Seconds: 1}); err != nil {
			return "", err
		}
		return event, nil
	})
}
