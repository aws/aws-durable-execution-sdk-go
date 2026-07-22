// Requirement 1-20: Error caught and handled (try/catch).
//
// From test-requirements/step/1-20.yaml:
//
//	description: Error caught and handled (try/catch)
//	handler: |
//	  A step that throws an error inside a try/catch block. The error is
//	  caught by user code and execution continues.
//	invocations: |
//	  - Handler wraps `context.step(failing_func, config=noRetry)` in
//	    try/catch, step fails, SDK checkpoints StepFailed, throws StepError,
//	    user code catches the error and continues with a fallback value,
//	    then a second step executes successfully.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	  Result: fallback_result
//
// Go has no try/catch; the idiomatic equivalent is checking the error
// return value from operations.Step directly - exactly what "catching" a
// StepError means here. The first step always fails with NoRetry (so it
// checkpoints StepFailed permanently, no RetryDetails), the handler
// observes the non-nil error and falls back to a literal value instead
// of propagating it, then a second, unrelated step succeeds normally -
// matching the YAML's two-step ExpectedExecutionHistory (StepFailed then
// StepSucceeded) and final SUCCEEDED result.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/utils"
)

func init() {
	Register("1-20", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_20Handler, config(client))
	})
}

func step1_20Handler(event any, dc types.DurableContext) (string, error) {
	fallback := "no_fallback_used"

	_, err := operations.Step(dc, "failing_func", func(sc types.StepContext) (string, error) {
		return "", errors.New("intentional failure for conformance requirement 1-20")
	}, operations.WithStepRetryStrategy[string](utils.Presets.NoRetry()))
	if err != nil {
		// "Caught": the error is observed and handled here rather than
		// propagated out of the handler, matching the YAML's try/catch
		// description.
		fallback = "fallback_result"
	}

	return operations.Step(dc, "second_step", func(sc types.StepContext) (string, error) {
		return fallback, nil
	})
}
