// Requirement 1-4: Returning complex object.
//
// From test-requirements/step/1-4.yaml:
//
//	description: Returning complex object
//	handler: |
//	  A step that returns a nested object with arrays and mixed types.
//	invocations: |
//	  - Handler invokes `context.step(build_response(event))`, step returns
//	    a nested object (e.g., {user: {name, tags: [...]}, count: N}), SDK
//	    serializes and deserializes the result correctly.
//	Input:
//	  name: Alice
//	  tags: [admin, active]
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	    user:
//	      name: Alice
//	      tags: [admin, active]
//	    count: 2
//
// Exercises the default JSON serdes's ability to round-trip a nested
// struct (map + slice + int) through the checkpointed step result -
// input is unmarshaled generically (types.DurableExecutionInvocationInput
// carries Input as json.RawMessage/any under the hood - see
// durable.WithDurableExecution's own generic Input type parameter) into a
// small typed struct so field access is straightforward, and the step's
// return value is a typed nested struct exercising real struct
// (de)serialization end to end, not just map[string]any passthrough.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// step1_4Input mirrors the YAML's Input shape ({name, tags}).
type step1_4Input struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// step1_4User mirrors the YAML's nested ExpectedResult.user shape.
type step1_4User struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// step1_4Response mirrors the YAML's full ExpectedResult shape.
type step1_4Response struct {
	User  step1_4User `json:"user"`
	Count int         `json:"count"`
}

func init() {
	Register("1-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_4Handler, config(client))
	})
}

func step1_4Handler(event step1_4Input, dc types.DurableContext) (step1_4Response, error) {
	return operations.Step(dc, "build_response", func(sc types.StepContext) (step1_4Response, error) {
		return step1_4Response{
			User: step1_4User{
				Name: event.Name,
				Tags: event.Tags,
			},
			Count: len(event.Tags),
		}, nil
	})
}
