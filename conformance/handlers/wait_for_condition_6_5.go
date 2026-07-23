// Requirement 6-5: Wait-for-condition fixed-delay wait strategy.
//
// From test-requirements/wait_for_condition/6-5.yaml:
//
//	description: Wait-for-condition with a fixed-delay wait strategy —
//	  each continue checkpoint records the constant next-attempt delay
//	handler: |
//	  Handler runs a single wait_for_condition operation configured with
//	  a fixed-delay wait strategy of 2 seconds (no jitter, no backoff).
//	  The check function increments an integer state starting from 0;
//	  the strategy continues while the state is below the input
//	  threshold (3) and stops once it reaches it. Each continue decision
//	  schedules the next poll exactly 2 seconds later. Returns the final
//	  state (3).
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    first check (0 -> 1), continue, SDK checkpoints StepSucceeded
//	    with state 1 and NextAttemptDelaySeconds 2, invocation completes,
//	    execution suspends for 2 seconds.
//	  - Replay 1/2: same pattern until the threshold (3) is reached;
//	    execution succeeds returning 3.
//	Input: 3
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: 3
//	ExpectedExecutionHistory: (each non-terminal StepSucceeded carries
//	  RetryDetails.NextAttemptDelaySeconds: 2)
//
// Uses operations.WithConditionRetryStrategy with utils.Presets.
// FixedDelay(2s, maxAttempts) - a large maxAttempts (10, well above the
// 3 polls this scenario actually needs) so the strategy always returns
// ShouldRetry:true with a constant 2-second delay for every attempt this
// test exercises, never itself triggering the max-attempts-exceeded
// path that 6-6 tests instead. pollAndCheckpoint threads
// decision.Delay's seconds straight into StepOptions.
// NextAttemptDelaySeconds on the RETRY checkpoint (see
// wait_for_condition.go), which the backend echoes back as
// StepSucceededDetails.RetryDetails.NextAttemptDelaySeconds in the event
// history - exactly what this requirement's ExpectedExecutionHistory
// asserts.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

func init() {
	Register("6-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_5Handler, config(client))
	})
}

func waitForCondition6_5Handler(threshold int, dc types.DurableContext) (int, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		next := state + 1
		return operations.ConditionResult[int]{State: next, ConditionMet: next >= threshold}, nil
	}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 2}, 10)))
}
