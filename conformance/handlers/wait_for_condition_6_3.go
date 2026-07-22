// Requirement 6-3: Wait-for-condition with explicit name.
//
// From test-requirements/wait_for_condition/6-3.yaml:
//
//	description: Wait-for-condition with an explicit operation name — the
//	  name is recorded on every poll event
//	handler: |
//	  Handler runs a single wait_for_condition operation given the
//	  explicit name "poll-status". The check function increments an
//	  integer state starting from 0; the wait strategy continues while
//	  the state is below 2 and stops once it reaches 2. Returns the final
//	  counter value (2).
//	invocations: |
//	  - Handler invokes wait_for_condition named "poll-status". SDK
//	    checkpoints StepStarted (SubType WaitForCondition, Name
//	    poll-status), runs the first check (0 -> 1), continue,
//	    StepSucceeded checkpointed with Name poll-status, invocation
//	    completes, execution suspends.
//	  - Replay 1: Second check (1 -> 2). State reaches the threshold, the
//	    wait strategy stops, SDK checkpoints the terminal StepSucceeded
//	    (Name poll-status), execution succeeds returning 2.
//	Input: 2
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: 2
//
// Identical shape to 6-1 but with the explicit operation name
// "poll-status" (the `id` argument to operations.WaitForCondition, which
// this SDK checkpoints as Name on every StepStarted/StepSucceeded event -
// see wait_for_condition.go's runWaitForCondition, which threads `name`
// straight through to every OperationUpdate it enqueues) and a threshold
// of 2 rather than 3, per this requirement's own YAML. Uses the same
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
	Register("6-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_3Handler, config(client))
	})
}

func waitForCondition6_3Handler(threshold int, dc types.DurableContext) (int, error) {
	return operations.WaitForCondition(dc, "poll-status", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		next := state + 1
		return operations.ConditionResult[int]{State: next, ConditionMet: next >= threshold}, nil
	}, 0, operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 1000)))
}
