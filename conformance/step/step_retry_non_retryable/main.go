// Command step_retry_non_retryable implements conformance requirement
// 1-16: a step that throws a custom TransientError which the retry
// strategy does not retry, so the step fails permanently.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// TransientError is the thrown error type, deliberately absent from the
// retry strategy's retryable set.
type TransientError struct {
	Message string
}

func (e *TransientError) Error() string { return e.Message }

// ValidationError is the only error type the retry strategy retries.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", &TransientError{Message: "transient failure"}
	}, durable.WithRetry(retryOnValidation()))
}

// retryOnValidation retries only ValidationError.
func retryOnValidation() durable.RetryStrategy {
	base := durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	})
	return func(a durable.RetryAttempt) durable.RetryDecision {
		var validation *ValidationError
		if !errors.As(a.Err, &validation) {
			return durable.RetryDecision{}
		}
		return base(a)
	}
}

func main() {
	durable.Start(handler)
}
