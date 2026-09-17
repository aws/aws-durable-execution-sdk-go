// Command step_retry_custom_config implements conformance requirement
// 1-14: a step that fails twice then succeeds, with a custom retry
// strategy (initial delay 2s, backoff rate 3x, no jitter), tracking
// attempts in DynamoDB.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/attempts"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	executionID := ctx.ExecutionArn()

	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		count, err := attempts.Increment(sc, executionID)
		if err != nil {
			return "", err
		}
		if count < 3 {
			return "", fmt.Errorf("Attempt %d failed", count)
		}
		return "finally succeeded", nil
	}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 2 * time.Second,
		BackoffRate:  3,
		Jitter:       durable.JitterNone,
	})))
}

func main() {
	durable.Start(handler)
}
