// Requirement 1-5: Undefined/null result.
//
// From test-requirements/step/1-5.yaml:
//
//	description: Undefined/null result
//	handler: |
//	  A step that returns null/undefined/None.
//	invocations: |
//	  - Handler invokes `context.step(do_nothing)`, step returns null.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//
// Go has no direct "null" value distinct from a type's zero value, so a
// step returning "nothing" is modeled as a step typed *string (a nil
// pointer), whose default JSON serdes serializes to the JSON literal
// null - the same wire representation every other SDK's None/undefined/
// null step result produces, matching ExpectedResult's empty (i.e. null)
// Result field.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("1-5", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_5Handler, config(client))
	})
}

func step1_5Handler(event any, dc types.DurableContext) (*string, error) {
	return operations.Step(dc, "do_nothing", func(sc types.StepContext) (*string, error) {
		return nil, nil
	})
}
