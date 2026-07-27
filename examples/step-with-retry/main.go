// Command step-with-retry demonstrates a step configured with a custom
// retry strategy using at-most-once semantics. The step fails its first two
// attempts and succeeds on the third, retrying with linearly increasing
// delays.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "flaky-operation",
		func(sc durable.StepContext) (string, error) {
			// Simulate two transient failures. Attempt is 1-based, so the
			// third attempt is the first that succeeds.
			if sc.Attempt() < 3 {
				return "", errors.New("transient failure")
			}
			return "step succeeded", nil
		},
		durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  4,
			InitialDelay: 1 * time.Second,
			BackoffRate:  1,
			Jitter:       durable.JitterNone,
		})),
		durable.WithSemantics(durable.AtMostOncePerRetry),
	)
}

func main() { durable.Start(handler) }
