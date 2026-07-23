// Requirement 3-6: Nested child contexts.
//
// From test-requirements/child/3-6.yaml:
//
//	description: Nested child contexts
//	handler: |
//	  A child context that contains another child context inside it, each
//	  with a step.
//	  The outer step returns the input string.
//	  The inner step returns the input string.
//	  The inner child context returns the inner step result.
//	  The outer child context returns the inner child context result.
//	invocations: |
//	  - Handler invokes an outer child context containing a step and a
//	    nested inner child context with its own step. Outer ContextStarted,
//	    outer step executes and succeeds with ParentId pointing to outer
//	    context Id, inner ContextStarted with ParentId pointing to outer
//	    context Id, inner step executes and succeeds with ParentId
//	    pointing to inner context Id, inner ContextSucceeded with ParentId
//	    pointing to outer context Id, outer ContextSucceeded.
//	Variables:
//	  INPUT_1: ${GEN_STR:8}
//	Input: ${INPUT_1}
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: ${INPUT_1}
//
// A RunInChildContext call whose fn itself calls a second, nested
// RunInChildContext - the outer step runs first (its result is
// discarded per the YAML's own "outer child context returns the inner
// child context result", not the outer step's), then the inner child
// context (with its own nested step) supplies the value the outer child
// context ultimately returns. Both child contexts' ParentId linkage
// (inner's CONTEXT/START checkpointed with ParentId = outer context's
// own operation ID) falls out automatically from c.NewChildWithName's
// hierarchical step-ID namespacing (see invoke.go's RunInChildContext
// doc) - the inner RunInChildContext call receives the outer call's own
// child DurableContext as ITS dc, so its own ParentStepID() resolves to
// the outer context's ID, exactly like the inner step in this same
// scenario.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-6", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_6Handler, config(client))
	})
}

func child3_6Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "outer_child", func(outer types.DurableContext) (string, error) {
		if _, err := operations.Step(outer, "outer_step", func(sc types.StepContext) (string, error) {
			return event, nil
		}); err != nil {
			return "", err
		}

		return operations.RunInChildContext(outer, "inner_child", func(inner types.DurableContext) (string, error) {
			return operations.Step(inner, "inner_step", func(sc types.StepContext) (string, error) {
				return event, nil
			})
		})
	})
}
