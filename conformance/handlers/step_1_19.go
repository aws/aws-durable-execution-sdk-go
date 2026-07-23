// Requirement 1-19: Step with error (fails permanently).
//
// From test-requirements/step/1-19.yaml:
//
//	description: Step with error (fails permanently)
//	handler: |
//	  A single step that always raises an exception, configured with no
//	  retries.
//	invocations: |
//	  - Handler invokes
//	    `context.step(failing_func, config=StepConfig(retry_strategy=RetryPresets.none()))`,
//	    step throws an error, no retry is attempted (max_attempts=1), step
//	    is checkpointed as failed.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: FAILED
//
// operations.WithStepRetryStrategy(utils.Presets.NoRetry()) is this SDK's
// exact equivalent of RetryPresets.none() - a strategy that always
// returns ShouldRetry: false, so the step's single failure is
// checkpointed as terminal (StepFailed) with no RetryDetails, and the
// enclosing execution fails.
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
	Register("1-19", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_19Handler, config(client))
	})
}

func step1_19Handler(event any, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "failing_func", func(sc types.StepContext) (string, error) {
		return "", errors.New("intentional permanent failure for conformance requirement 1-19")
	}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
}
