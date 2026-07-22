// Requirement 3-14: Child context with custom serdes (succeed).
//
// From test-requirements/child/3-14.yaml:
//
//	description: Child context with custom serdes (succeed)
//	handler: |
//	  A child context configured with a custom serdes that transforms the
//	  result to uppercase on serialization. The child context succeeds and
//	  the serialized result is returned.
//	  The step returns the input string.
//	  The child context returns the uppercased input via custom serdes.
//	invocations: |
//	  - Handler invokes a child context with a custom serdes that
//	    transforms the result to uppercase, containing a step that returns
//	    the input. Child step returns the input, custom serdes serializes
//	    the child context result as uppercase.
//	Input: hello child
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: HELLO CHILD
//	ExpectedExecutionHistory:
//	  ... StepSucceeded Result.Payload: '"hello child"'  (the STEP's own
//	      result is the plain default-serdes JSON string, unaffected)
//	  ... ContextSucceeded Result.Payload: HELLO CHILD  (note: NOT
//	      JSON-quoted - the custom serdes's own raw wire bytes, exactly
//	      like step_1_6.go's identical unquoted "HELLO WORLD" expectation)
//
// Reuses this package's existing uppercaseSerdes type (step_1_6.go, same
// package) via operations.WithChildSerdes - the real, exported per-child
// -context serdes override (confirmed directly from invoke.go's
// childConfig.serdes / WithChildSerdes, mirroring WithStepSerdes exactly)
// - rather than declaring a second, duplicate uppercasing serdes type.
// Only the child CONTEXT's own result is passed through uppercaseSerdes;
// the nested step still uses the default JSON serdes (WithStepSerdes is
// not used here), matching the YAML's own distinct StepSucceeded
// (JSON-quoted, unchanged) vs. ContextSucceeded (uppercased, custom wire
// format) expectations.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("3-14", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_14Handler, config(client))
	})
}

func child3_14Handler(event string, dc types.DurableContext) (string, error) {
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "step", func(sc types.StepContext) (string, error) {
			return event, nil
		})
	}, operations.WithChildSerdes[string](uppercaseSerdes{}))
}
