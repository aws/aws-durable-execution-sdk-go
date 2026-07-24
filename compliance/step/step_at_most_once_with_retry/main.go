// Conformance 1-18: AtMostOnce with retry, crash on first attempt.
package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/attempts"
)

func handler(ctx durable.Context, event string) (string, error) {
	executionID := ctx.ExecutionArn()
	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		count, err := attempts.Increment(sc, executionID)
		if err != nil {
			return "", err
		}
		fmt.Println(event)
		if count < 2 {
			os.Exit(1)
		}
		return "succeeded on second attempt", nil
	},
		durable.WithSemantics(durable.AtMostOncePerRetry),
		durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: 1,
			Jitter:       durable.JitterNone,
		})))
}

func main() {
	durable.Start(handler)
}
