// Requirement 3-1: Child context basic (single step inside).
//
// From test-requirements/child/3-1.yaml:
//
//	description: Child context basic (single step inside)
//	handler: |
//	  A child context containing a single step that succeeds.
//	  The step returns the input string.
//	  The child context returns the step result.
//	invocations: |
//	  - Handler invokes a child context with a step inside. SDK
//	    checkpoints ContextStarted, step executes and succeeds with
//	    ParentId pointing to child context Id, SDK checkpoints
//	    ContextSucceeded with the result.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// The simplest possible operations.RunInChildContext call: a single
// nested operations.Step whose result the child context itself returns
// unchanged. RunInChildContext's own SubType is the real, exported
// "RunInChildContext" constant (see invoke.go's subTypeRunInChildContext
// default), and the nested Step's ParentId is automatically the child
// context's own operation ID because fn receives a genuine child
// types.DurableContext (c.NewChildWithName) whose NextStepID calls derive
// from that child's own hierarchical namespace - nothing this handler
// needs to wire up manually.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("3-1", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_1Handler, config(client))
	})
}

func child3_1Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
			return event, nil
		})
	})
}
