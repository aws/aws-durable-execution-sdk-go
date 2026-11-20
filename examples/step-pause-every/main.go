// Command step-pause-every demonstrates a retry strategy that pauses the
// durable execution. The step fails its first three attempts. The strategy
// sets PauseEvery to 2, so the SDK pauses the execution after attempt 2.
// After an operator resumes the execution, attempt 3 fails and retries as
// usual, and attempt 4 succeeds.
//
// Pausing is experimental. The execution role needs
// lambda:PauseDurableExecution on its own durable executions. The example
// runs only on the local runner.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "flaky-operation",
		func(sc durable.StepContext) (string, error) {
			if sc.Attempt() < 4 {
				return "", fmt.Errorf("transient failure on attempt %d", sc.Attempt())
			}
			return fmt.Sprintf("succeeded on attempt %d", sc.Attempt()), nil
		},
		durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  5,
			InitialDelay: time.Second,
			BackoffRate:  1,
			Jitter:       durable.JitterNone,
			PauseEvery:   2,
		})),
	)
}

func main() { durable.Start(handler) }
