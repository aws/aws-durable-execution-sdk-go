// Command child_retry implements conformance requirement 3-7: a child
// context with a step that fails on the first attempt and succeeds on the
// second, using a retry strategy.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/attempts"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	executionID := ctx.ExecutionArn()

	return durable.RunInChildContext(ctx, "retry-child", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(sc durable.StepContext) (string, error) {
			count, err := attempts.Increment(sc, executionID)
			if err != nil {
				return "", err
			}
			if count < 2 {
				return "", fmt.Errorf("Attempt %d failed", count)
			}
			return event, nil
		}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: time.Second,
			Jitter:       durable.JitterNone,
		})))
	})
}

func main() {
	durable.Start(handler)
}
