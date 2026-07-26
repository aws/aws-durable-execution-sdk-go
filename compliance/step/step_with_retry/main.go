// Command step_with_retry implements conformance requirement 1-11: a step
// that fails on the first attempt and succeeds on the second, tracking
// attempts in DynamoDB.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/attempts"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	executionID := ctx.ExecutionArn()

	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		count, err := attempts.Increment(sc, executionID)
		if err != nil {
			return "", err
		}
		if count < 2 {
			return "", fmt.Errorf("Attempt %d failed", count)
		}
		return "Operation succeeded", nil
	}, durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Jitter:       durable.JitterNone,
	})))
}

func main() {
	durable.Start(handler)
}
