// Requirement 1-3: Sequential steps where second depends on first.
//
// From test-requirements/step/1-3.yaml:
//
//	description: Sequential steps where second depends on first
//	handler: |
//	  Two sequential steps where the second step uses the result of the
//	  first.
//	invocations: |
//	  - Handler invokes `result1 = context.step(step_one)`, first step
//	    succeeds, then invokes `result2 = context.step(step_two(result1))`,
//	    second step succeeds using first step's result.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: first_second
//
// Two plain operations.Step calls in sequence, the second closing over the
// first's returned value - exercises ordinary sequential composition and
// distinct hierarchical step IDs (${ID1}, ${ID2} in the expected history).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("1-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_3Handler, config(client))
	})
}

func step1_3Handler(event any, dc types.DurableContext) (string, error) {
	result1, err := operations.Step(dc, "step_one", func(sc types.StepContext) (string, error) {
		return "first", nil
	})
	if err != nil {
		return "", err
	}

	result2, err := operations.Step(dc, "step_two", func(sc types.StepContext) (string, error) {
		return result1 + "_second", nil
	})
	if err != nil {
		return "", err
	}

	return result2, nil
}
