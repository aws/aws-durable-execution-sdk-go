// Requirement 6-9: Wait-for-condition with a complex object state.
//
// From test-requirements/wait_for_condition/6-9.yaml:
//
//	description: Wait-for-condition that threads a structured object as
//	  its state and returns the final object when the condition is met
//	handler: |
//	  Handler runs a single wait_for_condition operation whose state is
//	  a structured object of the form {status, attempts}. The initial
//	  state is {status: PENDING, attempts: 0}. Each check increments
//	  attempts by 1 and, once attempts reaches 2, sets status to DONE.
//	  The wait strategy continues while status is PENDING and stops when
//	  it becomes DONE, returning the final object {status: DONE,
//	  attempts: 2}.
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    first check produces {status: PENDING, attempts: 1}, continue,
//	    StepSucceeded checkpointed, invocation completes, execution
//	    suspends.
//	  - Replay 1: Second check produces {status: DONE, attempts: 2}. The
//	    wait strategy stops, SDK checkpoints the terminal StepSucceeded
//	    with the final object, execution succeeds returning {status:
//	    DONE, attempts: 2}.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    status: DONE
//	    attempts: 2
//
// TState is a struct rather than a primitive here, exercising
// operations.WaitForCondition's own generic State type parameter with a
// real structured payload - both the JSON round trip on the wire
// (Payload) and the SDK's own state-carrying RETRY checkpoint (see
// wait_for_condition.go's Payload-on-RETRY design) go through
// DefaultSerdes' normal JSON marshal/unmarshal, requiring no special
// handling beyond correct json tags matching the YAML's own lowercase
// field names (status/attempts). Uses the same explicit,
// generously-bounded FixedDelay retry strategy 6-1 does - see that
// file's own doc for why the operation's zero-option default (NoRetry)
// cannot drive a genuine multi-poll scenario like this one.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

// waitForCondition6_9State mirrors the YAML's structured state shape
// ({status, attempts}), threaded across polls and returned as this
// requirement's own final Result.
type waitForCondition6_9State struct {
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
}

func init() {
	Register("6-9", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_9Handler, config(client))
	})
}

func waitForCondition6_9Handler(event any, dc types.DurableContext) (waitForCondition6_9State, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state waitForCondition6_9State) (operations.ConditionResult[waitForCondition6_9State], error) {
		next := waitForCondition6_9State{Status: state.Status, Attempts: state.Attempts + 1}
		if next.Attempts >= 2 {
			next.Status = "DONE"
		}
		return operations.ConditionResult[waitForCondition6_9State]{State: next, ConditionMet: next.Status == "DONE"}, nil
	}, waitForCondition6_9State{Status: "PENDING", Attempts: 0}, operations.WithConditionRetryStrategy[waitForCondition6_9State](utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 1000)))
}
