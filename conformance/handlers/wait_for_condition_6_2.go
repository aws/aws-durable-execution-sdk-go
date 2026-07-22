// Requirement 6-2: Wait-for-condition immediate stop (condition already
// met on first check).
//
// From test-requirements/wait_for_condition/6-2.yaml:
//
//	description: Wait-for-condition immediate — the condition is already
//	  satisfied on the first check, so polling stops without any
//	  suspension
//	handler: |
//	  Handler runs a single wait_for_condition operation with an initial
//	  state of 5. The check function returns the state unchanged. The
//	  wait strategy stops polling on the first attempt because the
//	  condition (state >= 5) is already satisfied, so the operation
//	  completes synchronously without suspending. Returns the final state
//	  (5).
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted
//	    (SubType WaitForCondition), runs the first check (state stays 5),
//	    the wait strategy stops immediately, SDK checkpoints the terminal
//	    StepSucceeded with the final state, the invocation completes, and
//	    execution succeeds returning 5. No suspend/resume cycle occurs.
//	Input: 5
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: 5
//
// checkFn reports ConditionMet: true on the very first invocation (state
// unchanged, already >= 5), so pollAndCheckpoint's ConditionMet==true
// branch checkpoints the terminal StepSucceeded directly with no RETRY
// checkpoint and no suspend at all - exactly one StepStarted/StepSucceeded
// pair, matching this requirement's own single-attempt
// ExpectedExecutionHistory.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("6-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_2Handler, config(client))
	})
}

func waitForCondition6_2Handler(event any, dc types.DurableContext) (int, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		return operations.ConditionResult[int]{State: state, ConditionMet: state >= 5}, nil
	}, 5)
}
