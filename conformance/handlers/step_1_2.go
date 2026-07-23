// Requirement 1-2: Step with name.
//
// From test-requirements/step/1-2.yaml:
//
//	description: Step with name
//	handler: |
//	  A single step with an explicit `name` parameter provided.
//	invocations: |
//	  - Handler invokes `context.step(func, name="custom_step_name")`, step
//	    succeeds on first attempt, returns the greeting result.
//	Input: World
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: Hello, World!
//
// operations.Step's second parameter (id) IS the step's name in this Go
// SDK - there is no separate name option (see step.go: the Step
// signature is Step(dc, id string, fn, opts...), and every checkpointed
// OperationUpdate uses that same string as its Name field). So "an
// explicit name parameter" here is satisfied simply by passing a
// non-default, custom id string ("custom_step_name") as the id argument,
// exactly as the YAML's ExpectedExecutionHistory's Name field expects.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("1-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_2Handler, config(client))
	})
}

func step1_2Handler(event string, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "custom_step_name", func(sc types.StepContext) (string, error) {
		return "Hello, " + event + "!", nil
	})
}
