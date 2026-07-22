// Requirement 6-12: Wait-for-condition followed by a step (result passed
// onward).
//
// From test-requirements/wait_for_condition/6-12.yaml:
//
//	description: Wait-for-condition whose final state feeds a subsequent
//	  step, verifying the resolved value flows into later operations and
//	  that the completed poll is not re-run after the resume
//	handler: |
//	  Handler first runs a wait_for_condition operation that increments
//	  an integer state from 0 and stops once it reaches the input
//	  threshold (2), yielding a final state of 2. The handler then
//	  passes that result into a step that multiplies it by 10. The
//	  execution succeeds returning 20.
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    first check (0 -> 1), continue, StepSucceeded checkpointed with
//	    state 1, invocation completes, execution suspends.
//	  - Replay 1: Second check (1 -> 2). The wait strategy stops, SDK
//	    checkpoints the terminal StepSucceeded (SubType WaitForCondition)
//	    with state 2. The handler then runs the step synchronously in the
//	    same invocation: SDK checkpoints StepStarted (SubType Step) and
//	    StepSucceeded (SubType Step) with result 20, and the execution
//	    succeeds returning 20.
//	Input: 2
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: 20
//
// The completed WaitForCondition's own result (state 2) is passed
// directly as input to a subsequent operations.Step that multiplies it
// by 10. On replay 1, WaitForCondition's own step ID replay-skips its
// completed poll loop entirely (types.OperationStatusSucceeded branch in
// runWaitForCondition - see wait_for_condition.go) and returns its
// cached final state immediately, so the "step" operation - a distinct
// step ID - executes for the first time in that SAME invocation right
// after, matching this requirement's "the handler then runs the step
// synchronously in the same invocation" and confirming the completed
// poll is never re-run after the resume. Uses the same explicit,
// generously-bounded FixedDelay retry strategy 6-1 does - see that
// file's own doc for why the operation's zero-option default (NoRetry)
// cannot drive this scenario's second poll.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

func init() {
	Register("6-12", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_12Handler, config(client))
	})
}

func waitForCondition6_12Handler(threshold int, dc types.DurableContext) (int, error) {
	polled, err := operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		next := state + 1
		return operations.ConditionResult[int]{State: next, ConditionMet: next >= threshold}, nil
	}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 1000)))
	if err != nil {
		return 0, err
	}

	return operations.Step(dc, "step", func(sc types.StepContext) (int, error) {
		return polled * 10, nil
	})
}
