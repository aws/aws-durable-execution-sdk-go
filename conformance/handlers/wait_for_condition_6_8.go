// Requirement 6-8: Wait-for-condition check throws, caught by handler
// (recovers).
//
// From test-requirements/wait_for_condition/6-8.yaml:
//
//	description: Wait-for-condition whose check raises on the first
//	  attempt, but the handler catches the failure and returns a
//	  fallback value, so the execution succeeds
//	handler: |
//	  Handler wraps a single wait_for_condition operation in a
//	  try/catch. The check function raises an exception on its first
//	  invocation; the SDK records a terminal StepFailed (SubType
//	  WaitForCondition) and re-raises. The handler catches the error and
//	  returns the fallback string "recovered", so the execution
//	  succeeds.
//	invocations: |
//	  - Handler invokes wait_for_condition. SDK checkpoints StepStarted,
//	    the first check raises, SDK checkpoints the terminal StepFailed
//	    (SubType WaitForCondition), and the error propagates to the
//	    handler. The handler catches it and returns "recovered"; the
//	    invocation completes and the execution succeeds.
//	Input: null
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: recovered
//
// Same checkFn-always-errors shape as 6-7 (NoRetry default, single
// terminal StepFailed checkpoint), but here the returned error is
// observed ("caught") and intentionally not propagated - the handler
// returns the fixed fallback string "recovered" instead, so
// durable.WithDurableExecution sees a nil error and marks the execution
// SUCCEEDED with that fallback as its Result, matching
// ExpectedResult.Result: recovered.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func init() {
	Register("6-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(waitForCondition6_8Handler, config(client))
	})
}

func waitForCondition6_8Handler(event any, dc types.DurableContext) (string, error) {
	_, condErr := operations.WaitForCondition(dc, "poll", func(sc types.StepContext, state int) (operations.ConditionResult[int], error) {
		return operations.ConditionResult[int]{}, errors.New("intentional check failure for conformance requirement 6-8")
	}, 0)
	// "Caught": condErr is observed here and intentionally not
	// propagated - this is the try/catch the YAML describes.
	_ = condErr

	return "recovered", nil
}
