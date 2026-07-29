// Conformance 1-18: AtMostOnce with retry, crash on first attempt.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/attempts"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	executionID := ctx.ExecutionArn()
	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		count, err := attempts.Increment(sc, executionID)
		if err != nil {
			return "", err
		}
		// Raw stdout write: the SDK's context logger suppresses emissions during
		// replay, and custom runtimes do not get platform-injected execution metadata.
		fmt.Printf("{\"executionArn\":%q,\"message\":%q}\n", executionID, event)
		if count < 2 {
			os.Exit(1)
		}
		return "succeeded on second attempt", nil
	},
		durable.WithSemantics(durable.AtMostOncePerRetry),
		durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  3,
			InitialDelay: time.Second,
			Jitter:       durable.JitterNone,
		})))
}

func main() {
	durable.Start(handler)
}
