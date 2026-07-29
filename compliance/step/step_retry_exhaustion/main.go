// Command step_retry_exhaustion implements conformance requirement 1-12: a
// step that always fails, with 4 total attempts at a fixed 1-second delay.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", errors.New("Always fails")
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  4,
		InitialDelay: time.Second,
		BackoffRate:  1,
		Jitter:       durable.JitterNone,
	})))
}

func main() {
	durable.Start(handler)
}
