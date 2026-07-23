// Requirement 6-6: Wait-for-condition max attempts exceeded (failure).
//
// From test-requirements/wait_for_condition/6-6.yaml:
//
//	description: Wait-for-condition where the condition is never met and
//	  the wait strategy exhausts its maximum attempts, failing the
//	  execution
//	handler: |
//	  Handler runs a single wait_for_condition operation configured with
//	  a fixed-delay wait strategy limited to a maximum of 3 attempts.
//	  The check function keeps incrementing an integer state and the
//	  condition is never satisfied, so the wait strategy exhausts its
//	  attempt budget. On the final attempt the strategy raises a
//	  max-attempts-exceeded failure, which the SDK records as a terminal
//	  StepFailed and the execution fails. The handler does not catch the
//	  error.
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    first check runs, continue, StepSucceeded checkpointed,
//	    invocation completes, execution suspends.
//	  - Replay 1: Second check runs, continue, StepSucceeded
//	    checkpointed, execution suspends.
//	  - Replay 2: Third (final permitted) check runs. The wait strategy
//	    has reached its maximum attempts and raises a
//	    max-attempts-exceeded error; SDK checkpoints the terminal
//	    StepFailed (SubType WaitForCondition) and the execution fails.
//	Input: 0
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// checkFn's condition (state >= 1000) is deliberately unreachable within
// 3 attempts - the condition is never met, so every attempt drives
// cfg.retryStrategy via the errConditionNotMet sentinel (see
// wait_for_condition.go's pollAndCheckpoint). utils.Presets.FixedDelay(
// 2s, 3) returns ShouldRetry:true for attempt 1 and 2 (attempt <
// maxAttempts), then ShouldRetry:false for attempt 3 (attempt >=
// maxAttempts), matching this requirement's own "third (final
// permitted) check... raises a max-attempts-exceeded error" - exactly
// two StepSucceeded (continue) checkpoints followed by one terminal
// StepFailed, per its ExpectedExecutionHistory. The handler does not
// observe/catch the returned error at all - it is propagated directly as
// the whole execution's own error, causing durable.WithDurableExecution
// to mark the execution FAILED, matching ExpectedResult.ExecutionStatus:
// FAILED.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

func init() {
	Register("6-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_6Handler, config(client))
	})
}

func waitForCondition6_6Handler(event any, dc types.DurableContext) (int, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		next := state + 1
		return operations.ConditionResult[int]{State: next, ConditionMet: next >= 1000}, nil
	}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 2}, 3)))
}
