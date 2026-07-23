// Requirement 3-18: Child context with step and wait inside, step and
// wait after.
//
// From test-requirements/child/3-18.yaml:
//
//	description: Child context with step and wait inside, step and wait
//	  after
//	handler: |
//	  A child context containing a step followed by a wait, then after
//	  the child context completes, a step and a wait at the top level.
//	  Validates that mixed operations work correctly both inside and
//	  after a child context across multiple replay cycles.
//	  The inner step returns the input string.
//	  The child context returns the input string.
//	  The outer step returns the input string.
//	invocations: |
//	  - Handler invokes a child context containing a step followed by a
//	    wait. ContextStarted, StepStarted with ParentId pointing to child
//	    context Id, StepSucceeded, WaitStarted with ParentId pointing to
//	    child context Id, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because child wait completed. Inner step is
//	    skipped, WaitSucceeded, ContextSucceeded, outer step executes and
//	    succeeds, outer WaitStarted, invocation completes, execution
//	    suspends.
//	  - Replay 2: Re-invoked because outer wait completed. Child and step
//	    are skipped, WaitSucceeded, execution succeeds.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// Combines 3-10's "step then wait inside a child context" shape with a
// second, top-level step-then-wait pair AFTER the child context - three
// full suspend/resume cycles total (AsyncInvoke is not set on this
// requirement's own YAML, so - per this task's briefing on
// multi-replay-cycle scenarios - the conformance runner's own async
// polling drives this handler through all three real Lambda invocations
// automatically; this handler code is identical in shape to 3-10 plus a
// second, un-nested step+wait pair, with no special multi-invocation
// logic needed here beyond simply issuing the four operations - inner
// step, inner wait, outer step, outer wait - in the documented order).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-18", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_18Handler, config(client))
	})
}

func child3_18Handler(event string, dc types.DurableContext) (string, error) {
	_, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		if _, err := operations.Step(child, "inner_step", func(sc types.StepContext) (string, error) {
			return event, nil
		}); err != nil {
			return "", err
		}
		if err := operations.Wait(child, "inner_wait", types.Duration{Seconds: 1}); err != nil {
			return "", err
		}
		return event, nil
	})
	if err != nil {
		return "", err
	}

	if _, err := operations.Step(dc, "outer_step", func(sc types.StepContext) (string, error) {
		return event, nil
	}); err != nil {
		return "", err
	}

	if err := operations.Wait(dc, "outer_wait", types.Duration{Seconds: 1}); err != nil {
		return "", err
	}

	return event, nil
}
