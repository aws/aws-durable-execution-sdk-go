// Requirement 6-4: Wait-for-condition custom initial state (state
// threaded across polls).
//
// From test-requirements/wait_for_condition/6-4.yaml:
//
//	description: Wait-for-condition with a non-zero initial state — the
//	  configured initial state seeds the first check and each returned
//	  state feeds the next
//	handler: |
//	  Handler runs a single wait_for_condition operation configured with
//	  an initial state of 5. The check function increments the state by
//	  1 on each invocation; the returned state is threaded into the next
//	  check. The wait strategy continues while the state is below the
//	  input threshold (8) and stops once it reaches it, returning the
//	  final state (8).
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    first check seeds with the initial state 5 (5 -> 6), continue,
//	    StepSucceeded checkpointed with state 6, invocation completes,
//	    execution suspends.
//	  - Replay 1/2: state threads 6 -> 7 -> 8, stopping once the
//	    threshold (8) is reached; execution succeeds returning 8.
//	Input: 8
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: 8
//
// Same increment-to-threshold shape as 6-1, but with initialState=5
// (rather than 0) passed as WaitForCondition's own initialState
// argument, confirming the SDK correctly seeds checkFn's first call with
// the caller-supplied initial state rather than a zero value (see
// wait_for_condition.go's own doc: "initialState seeds the first
// invocation of checkFn and must be provided explicitly"). Uses the same
// explicit, generously-bounded FixedDelay retry strategy 6-1 does - see
// that file's own doc for why the operation's zero-option default
// (NoRetry) cannot drive a genuine multi-poll scenario like this one.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

func init() {
	Register("6-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_4Handler, config(client))
	})
}

func waitForCondition6_4Handler(threshold int, dc types.DurableContext) (int, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		next := state + 1
		return operations.ConditionResult[int]{State: next, ConditionMet: next >= threshold}, nil
	}, 5, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 1000)))
}
