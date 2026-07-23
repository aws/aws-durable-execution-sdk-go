// Requirement 5-4: Invoke returning null (target echoes null input).
//
// From test-requirements/invoke/5-4.yaml:
//
//	description: Invoke returning null — echo target receives null and returns null
//	handler: |
//	  A handler that invokes an echo target function with null as the payload.
//	  The target function returns null. The handler returns the null result.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//
// Same shape as 5-3 (any/any, default serdes), but exercising the `null`
// input/output edge case specifically: echo-target's own main.go probes
// the raw event bytes for its reserved control-key object shape via
// json.Unmarshal into a map, which simply fails (silently, by design) for
// a literal `null` payload, falling through to echo the raw `null` bytes
// back unchanged - so this requirement needs no special handling beyond
// what 5-1/5-3 already do; it exists to prove the null case specifically
// rather than to exercise new SDK behavior.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("5-4", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(invoke5_4Handler, config(client))
	})
}

func invoke5_4Handler(event any, dc types.DurableContext) (any, error) {
	return operations.Invoke[any, any](dc, "invoke", echoTargetFunctionARN(), event)
}
