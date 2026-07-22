// Command child_retry_exhaustion implements conformance requirement 3-8: a
// child context with a step that always fails; after retries are exhausted
// the child context is marked failed.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.RunInChildContext(ctx, "exhaust-child", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return "", errors.New("Always fails")
		}, durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  2,
			InitialDelay: time.Second,
			BackoffRate:  1,
			Jitter:       durable.JitterNone,
		})))
	})
}

func main() {
	durable.Start(handler)
}
