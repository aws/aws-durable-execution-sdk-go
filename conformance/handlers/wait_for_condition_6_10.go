// Requirement 6-10: Wait-for-condition null result.
//
// From test-requirements/wait_for_condition/6-10.yaml:
//
//	description: Wait-for-condition that stops polling on the first
//	  check and returns a null final state
//	handler: |
//	  Handler runs a single wait_for_condition operation whose check
//	  function returns null. The wait strategy stops polling on the
//	  first attempt, so the operation completes synchronously with a
//	  null final state and the execution succeeds returning null.
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted
//	    (SubType WaitForCondition), the first check returns null, the
//	    wait strategy stops, SDK checkpoints the terminal StepSucceeded
//	    with the null state, the invocation completes, and the execution
//	    succeeds returning null.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: null
//
// TState is *string (a nil pointer, rather than a primitive) so a
// genuinely null/absent state is representable at all - Go has no
// nil-able int/bool, and this requirement's own scenario is specifically
// about a null state, not an empty-but-present one. checkFn reports
// ConditionMet: true immediately with State: nil on the very first
// attempt, so this is a single StepStarted/StepSucceeded pair with no
// suspend, structurally identical to 6-2's immediate-stop shape but with
// a nil payload rather than a real value - DefaultSerdes' JSON
// marshaling of a nil *string produces the JSON literal `null`, matching
// StepSucceededDetails.Result and the execution's own final
// ExpectedResult.Result: null.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("6-10", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_10Handler, config(client))
	})
}

func waitForCondition6_10Handler(event any, dc types.DurableContext) (*string, error) {
	return operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state *string) (operations.ConditionResult[*string], error) {
		return operations.ConditionResult[*string]{State: nil, ConditionMet: true}, nil
	}, nil)
}
