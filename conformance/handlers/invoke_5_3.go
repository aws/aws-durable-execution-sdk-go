// Requirement 5-3: Invoke returning complex object (nested JSON).
//
// From test-requirements/invoke/5-3.yaml:
//
//	description: Invoke returning complex object — echo target returns the nested JSON
//	  input as-is
//	handler: |
//	  A handler that invokes an echo target function with a complex nested JSON input.
//	  The target function returns the input unchanged. The handler returns the invoke result.
//	Input:
//	  nested:
//	    key: value
//	    items: [1, 2, 3]
//	  source: test
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Identical shape to 5-1 (a single, unnamed operations.Invoke call
// returning the echo target's result verbatim) except the input/output
// type is an arbitrary nested JSON object rather than a bare string -
// using `any` for both TIn/TOut lets the default JSON serdes (see
// utils.JSONSerdes) round-trip whatever shape the conformance runner
// sends without this handler needing to know its schema, and
// echo-target's own main.go deliberately echoes raw JSON bytes
// unmodified (see that file's own doc comment on why byte-for-byte
// fidelity matters here), so no intermediate Go struct is needed at all.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("5-3", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_3Handler, config(client))
	})
}

func invoke5_3Handler(event any, dc types.DurableContext) (any, error) {
	return operations.Invoke[any, any](dc, "invoke", echoTargetFunctionARN(), event)
}
