// Requirement 6-13: Multiple sequential wait_for_condition operations.
//
// From test-requirements/wait_for_condition/6-13.yaml:
//
//	description: Two sequential wait_for_condition operations in one
//	  handler — each is a distinct operation with its own Id, and the
//	  first result seeds the second
//	handler: |
//	  Handler runs two wait_for_condition operations back to back. The
//	  first starts from an initial state of 0, increments the state by 1
//	  on each invocation, and stops once the state reaches 2, returning
//	  2. That result seeds the second operation's initial state; the
//	  second increments and stops once the state reaches 4, returning 4.
//	  The handler returns the second operation's result.
//	invocations: |
//	  - Fresh invoke: first wait_for_condition checkpoints StepStarted
//	    (Id ${ID1}), first check 0 -> 1, continue, StepSucceeded
//	    checkpointed with state 1, invocation completes, execution
//	    suspends.
//	  - Replay 1: first operation resumes, second check 1 -> 2, reaches
//	    threshold, terminal StepSucceeded for ${ID1} with state 2; the
//	    first operation returns 2. The handler then starts the second
//	    wait_for_condition in the same invocation: StepStarted (new Id
//	    ${ID2}), first check 2 -> 3, continue, StepSucceeded checkpointed
//	    with state 3, invocation completes, execution suspends.
//	  - Replay 2: second operation resumes, check 3 -> 4, reaches
//	    threshold, terminal StepSucceeded for ${ID2} with state 4,
//	    execution succeeds returning 4.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: 4
//
// Two independent operations.WaitForCondition calls under distinct
// operation names ("poll-1"/"poll-2"), each minting its own step ID via
// dc.NextStepID() (see operations.WaitForCondition's own call to that
// method) - the SAME mechanism that already gives 6-1's single call and
// child_3-suite's multiple RunInChildContext calls their own distinct
// IDs, so no extra bookkeeping is needed here beyond simply issuing the
// two calls in sequence with the second's initialState seeded from the
// first's returned final state (2), each polling to its own,
// independent threshold (4, i.e. two further increments past the
// seeded 2). Both calls use the same explicit, generously-bounded
// FixedDelay retry strategy 6-1 does - see that file's own doc for why
// the operation's zero-option default (NoRetry) cannot drive either
// operation's second poll.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

func init() {
	Register("6-13", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_13Handler, config(client))
	})
}

func waitForCondition6_13Handler(event any, dc types.DurableContext) (int, error) {
	incrementToThreshold := func(threshold int) func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		return func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
			next := state + 1
			return operations.ConditionResult[int]{State: next, ConditionMet: next >= threshold}, nil
		}
	}
	retry := operations.WithConditionRetryStrategy[int](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 1000))

	first, err := operations.WaitForCondition(dc, "poll-1", incrementToThreshold(2), 0, retry)
	if err != nil {
		return 0, err
	}

	return operations.WaitForCondition(dc, "poll-2", incrementToThreshold(4), first, retry)
}
