// Requirement 3-8: Child context with step retry exhaustion (child
// fails).
//
// From test-requirements/child/3-8.yaml:
//
//	description: Child context with step retry exhaustion (child fails)
//	handler: |
//	  A child context containing a step that always fails. After retries
//	  are exhausted, the child context is marked as failed.
//	invocations: |
//	  - Handler invokes a child context with a step that always fails
//	    (maxAttempts=2). ContextStarted, StepStarted, step fails,
//	    StepFailed with RetryDetails including NextAttemptDelaySeconds,
//	    invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because retry delay elapsed. StepStarted,
//	    step fails again, StepFailed with RetryDetails (no
//	    NextAttemptDelaySeconds means retries exhausted), ContextFailed,
//	    execution fails.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// utils.Presets.FixedDelay(1s, 2) caps the step at 2 total attempts, both
// of which fail unconditionally - the second attempt's retry strategy
// call then returns ShouldRetry: false (maxAttempts reached), producing
// the permanent StepFailed with no NextAttemptDelaySeconds the YAML
// describes, which in turn makes RunInChildContext's own error path
// checkpoint ContextFailed and return a *ChildContextFailedError this
// handler propagates as the whole execution's failure.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/utils"
)

func init() {
	Register("3-8", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(child3_8Handler, config(client))
	})
}

func child3_8Handler(event any, dc types.DurableContext) (*string, error) {
	strategy := utils.Presets.FixedDelay(types.Duration{Seconds: 1}, 2)
	_, err := operations.RunInChildContext(dc, "child", func(child types.DurableContext) (string, error) {
		return operations.Step(child, "always_fails", func(sc types.StepContext) (string, error) {
			return "", errors.New("intentional permanent failure for conformance requirement 3-8")
		}, operations.WithStepRetryStrategy[string](strategy))
	})
	return nil, err
}
