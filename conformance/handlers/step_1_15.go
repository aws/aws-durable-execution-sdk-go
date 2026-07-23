// Requirement 1-15: Retry specific exception.
//
// From test-requirements/step/1-15.yaml:
//
//	description: Retry specific exception
//	handler: |
//	  A step that throws a custom TransientError on the first attempt. The
//	  retry strategy retries only when the error is an instance of
//	  TransientError.
//	invocations: |
//	  - Handler invokes `context.step(func, { retryStrategy: retry_on_transient })`,
//	    step throws TransientError, strategy checks error type, returns
//	    shouldRetry=true, invocation completes, execution suspends.
//	  - Replay 1: Re-invoked because retry delay elapsed, SDK replays,
//	    re-executes step, step succeeds, execution succeeds.
//	Input:
//	ExpectedResult:
//	  ExecutionStatus: SUCCEEDED
//	ExpectedExecutionHistory:
//	  ...
//	  RetryDetails:
//	    CurrentAttempt: 1
//	    NextAttemptDelaySeconds: 1
//
// Reuses the *TransientError type declared in step_1_16.go (this
// requirement's mirror image: same error type, opposite retry decision)
// - retryOnTransient below returns ShouldRetry: true (with a fixed 1s
// delay, matching the YAML's NextAttemptDelaySeconds: 1) specifically
// for *TransientError via errors.As, demonstrating the same
// error-type-conditional retry strategy shape as 1-16, just deciding to
// retry instead of declining.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("1-15", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(step1_15Handler, config(client))
	})
}

// retryOnTransient retries exactly once (matching the YAML's two-attempt
// scenario) for a *TransientError, with a fixed 1-second delay.
func retryOnTransient(err error, attempt int) types.RetryDecision {
	var transient *TransientError
	if errors.As(err, &transient) && attempt < 2 {
		d := types.Duration{Seconds: 1}
		return types.RetryDecision{ShouldRetry: true, Delay: &d}
	}
	return types.RetryDecision{ShouldRetry: false}
}

func step1_15Handler(event any, dc types.DurableContext) (string, error) {
	return operations.Step(dc, "func", func(sc types.StepContext) (string, error) {
		if sc.Attempt() < 2 {
			return "", &TransientError{Message: "intentional transient-typed failure for conformance requirement 1-15"}
		}
		return "succeeded after transient retry", nil
	}, operations.WithStepRetryStrategy[string](retryOnTransient))
}
