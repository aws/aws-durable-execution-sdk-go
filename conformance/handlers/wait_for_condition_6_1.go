// Requirement 6-1: Wait-for-condition basic (polls until threshold met).
//
// From test-requirements/wait_for_condition/6-1.yaml:
//
//	description: Wait-for-condition basic — polls an incrementing counter
//	  until it reaches the input threshold
//	handler: |
//	  Handler runs a single wait_for_condition operation. The check
//	  function starts from an initial integer state of 0 and increments
//	  the state by 1 on each invocation. The wait strategy continues
//	  polling while the state is below the input threshold (3) and stops
//	  once the state reaches it, returning the final counter value.
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted
//	    (SubType WaitForCondition), runs the first check (state 0 -> 1),
//	    the wait strategy returns continue, SDK checkpoints StepSucceeded
//	    carrying the new state plus a next-attempt delay, invocation
//	    completes, execution suspends.
//	  - Replay 1/2: same pattern until the state reaches the threshold
//	    (3), at which point the terminal StepSucceeded is checkpointed and
//	    the execution succeeds returning 3.
//	Input: 3
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: 3
//
// The simplest possible operations.WaitForCondition call: checkFn
// increments the int state by 1 and reports ConditionMet once it reaches
// the input threshold. Uses utils.Presets.FixedDelay with a generously
// high maxAttempts (this scenario only ever needs 3) as its retry
// strategy, rather than relying on this operation's own zero-option
// default: WaitForCondition's real, current zero-option default is
// utils.Presets.NoRetry() (see wait_for_condition.go's cfg
// initialization) - i.e. "never continue," not "continue with a default
// delay" as this file originally assumed. Confirmed directly via a real
// deployment against a live account (730758745077, us-east-1): with no
// retry strategy configured, this exact scenario failed on its very
// first poll with a terminal StepFailed (errConditionNotMet's own
// "condition not yet met" message), never reaching a second attempt at
// all - a genuinely different, incorrect ExecutionFailed outcome from
// this requirement's own documented multi-poll ExpectedExecutionHistory.
// This requirement's own "wait strategy continues... and stops" prose
// describes an abstract polling policy driven purely by whether the
// condition is met, distinct from a bounded-attempts retry strategy -
// FixedDelay's large maxAttempts here exists only to satisfy this SDK's
// concrete retry-strategy API shape (which always requires SOME
// eventual give-up bound) without that bound ever actually being reached
// by this scenario's 3 real polls.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

func init() {
	Register("6-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_1Handler, config(client))
	})
}

func waitForCondition6_1Handler(threshold int, dc types.DurableContext) (int, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		next := state + 1
		return operations.ConditionResult[int]{State: next, ConditionMet: next >= threshold}, nil
	}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 1000)))
}
