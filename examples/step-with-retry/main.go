// Command step-with-retry demonstrates a step configured with a custom
// retry strategy using at-most-once semantics. The step simulates a
// transient failure (50% chance) and retries up to 3 times with linearly
// increasing delays.
package main

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "flaky-operation",
		func(_ durable.StepContext) (string, error) {
			if rand.Float64() < 0.5 { //nolint:gosec // intentional randomness for demo
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
