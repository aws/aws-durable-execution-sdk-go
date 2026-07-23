// Requirement 3-2: Child context with name.
//
// From test-requirements/child/3-2.yaml:
//
//	description: Child context with name
//	handler: |
//	  A named child context that uses the first input string as its name,
//	  containing a single step that succeeds. The step returns the second
//	  input string.
//	  The child context returns the step result.
//	invocations: |
//	  - Handler invokes a named child context using the first input as the
//	    name, with a step inside. SDK checkpoints ContextStarted with Name
//	    set to the first input, step executes and succeeds with ParentId
//	    pointing to child context Id, SDK checkpoints ContextSucceeded with
//	    Name set to the first input.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	  INPUT_2: ${GEN_STR:8}
//	Input:
//	  name: ${INPUT_1}
//	  value: ${INPUT_2}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_2}
//
// RunInChildContext's second parameter (id) IS the child context's
// checkpointed Name (see invoke.go: the CONTEXT/START OperationUpdate's
// Name field is set to that same id argument) - exactly like Step's id
// doubling as its Name. So "uses the first input string as its name"
// means passing event.Name (the first input field) directly as
// RunInChildContext's id argument, and the nested step returns
// event.Value (the second input field) unchanged.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// child3_2Input mirrors the YAML's Input shape ({name, value}).
type child3_2Input struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func init() {
	Register("3-2", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_2Handler, config(client))
	})
}

func child3_2Handler(event child3_2Input, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, event.Name, func(child types.DurableContext) (string, error) {
		return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
			return event.Value, nil
		})
	})
}
