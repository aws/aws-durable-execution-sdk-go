// Requirement 1-1: Step basic (succeeds on first attempt).
//
// From test-requirements/step/1-1.yaml:
//
//	description: Step basic (succeeds on first attempt)
//	handler: |
//	  A single step that takes input and returns a greeting string.
//	invocations: |
//	  - Handler invokes `context.step(greet(event))`, step succeeds on
//	    first attempt, returns the greeting result.
//	Input: World
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: Hello, World!
//
// A single context.Step call that always succeeds on its first attempt,
// returning "Hello, <input>!" - the simplest possible durable operation,
// and the first requirement implemented in this harness (see
// ../registry.go's own doc comment for the shared registration
// contract every other requirement file follows identically).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("1-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_1Handler, config(client))
	})
}

func step1_1Handler(event string, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "greet", func(sc types.StepContext) (string, error) {
		return "Hello, " + event + "!", nil
	})
}
