// Command step_retry_specific_exception implements conformance requirement
// 1-15: a step that throws a custom TransientError on the first attempt;
// the retry strategy retries only TransientError.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/attempts"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// TransientError is the custom error type that the retry strategy treats
// as retryable.
type TransientError struct {
	Message string
}

func (e *TransientError) Error() string { return e.Message }

func handler(ctx durable.Context, _ any) (string, error) {
	executionID := ctx.ExecutionArn()

	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		count, err := attempts.Increment(sc, executionID)
		if err != nil {
			return "", err
		}
		if count < 2 {
			return "", &TransientError{Message: "Temporary failure"}
		}
		return "recovered from transient", nil
	}, durable.WithRetry(retryOnTransient()))
}

// retryOnTransient retries only TransientError, up to 3 attempts with a
// deterministic 1-second delay.
func retryOnTransient() durable.RetryStrategy {
	base := durable.NewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	})
	return func(err error, attempt int) durable.RetryDecision {
		var transient *TransientError
		if !errors.As(err, &transient) {
			return durable.RetryDecision{}
		}
		return base(err, attempt)
	}
}

func main() {
	durable.Start(handler)
}
