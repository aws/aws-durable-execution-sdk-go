// Requirement 3-3: Child context with multiple sequential steps.
//
// From test-requirements/child/3-3.yaml:
//
//	description: Child context with multiple sequential steps
//	handler: |
//	  A child context containing two sequential steps where the second
//	  receives the first step's result as input.
//	  The first step returns the input string.
//	  The second step receives the first step's result and returns it
//	  unchanged.
//	  The child context returns the second step result.
//	invocations: |
//	  - Handler invokes a child context with two sequential steps inside
//	    where the second depends on the first. SDK checkpoints
//	    ContextStarted, first step executes and succeeds with ParentId
//	    pointing to child context Id, second step executes and succeeds
//	    with ParentId pointing to child context Id, SDK checkpoints
//	    ContextSucceeded with the final result.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// Two sequential operations.Step calls nested inside one
// operations.RunInChildContext, each independently checkpointed under
// the child context's own step-ID namespace (distinct ids "step-1" and
// "step-2" so their hashed step IDs never collide) - the second step's
// closure simply reads the first step's already-returned Go value
// directly (ordinary sequential Go data flow), which is exactly "the
// second receives the first step's result as input" for a same-process,
// same-invocation data dependency; no extra plumbing is needed since
// both steps run within the same child fn closure.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_3Handler, config(client))
	})
}

func child3_3Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		first, err := operations.Step(child, "step-1", func(sc types.StepContext) (string, error) {
			return event, nil
		})
		if err != nil {
			return "", err
		}
		return operations.Step(child, "step-2", func(sc types.StepContext) (string, error) {
			return first, nil
		})
	})
}
