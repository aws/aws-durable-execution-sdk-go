// Requirement 3-16: Child context returning null.
//
// From test-requirements/child/3-16.yaml:
//
//	description: Child context returning null
//	handler: |
//	  A child context where the child function directly returns null
//	  without calling any durable operation. Tests that the serdes
//	  handles null/undefined correctly.
//	invocations: |
//	  - Handler invokes a child context where the child function returns
//	    null without calling any durable operation. SDK checkpoints
//	    ContextStarted, child returns null directly, SDK checkpoints
//	    ContextSucceeded, execution succeeds with null output.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result:
//	ExpectedExecutionHistory:
//	  - EventType: ContextStarted ...
//	  - EventType: ContextSucceeded ... ContextSucceededDetails: {}
//	  - EventType: InvocationCompleted ...
//
// Same "Go has no direct null distinct from a zero value" modeling
// step_1_5.go already established for a plain Step's null result: T is
// *string here so fn's direct `return nil, nil` (no nested operation of
// any kind, matching the YAML's own "without calling any durable
// operation") serializes to the JSON literal null via
// RunInChildContext's default JSON serdes, producing an empty/omitted
// Result field on the checkpointed ContextSucceededDetails - matching
// ContextSucceededDetails: {} exactly (checkResultSize's serialized
// length for the 4-byte JSON literal "null" is trivially under the
// threshold, so no oversized-result concern applies here).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("3-16", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_16Handler, config(client))
	})
}

func child3_16Handler(event any, dc types.DurableContext) (*string, error) {
	return operations.RunInChildContext(dc, "child", func(child types.DurableContext) (*string, error) {
		return nil, nil
	})
}
