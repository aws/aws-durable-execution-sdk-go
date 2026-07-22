// Command retry-exhaustion demonstrates a step that always fails, exhausting
// all retry attempts. Once the retry strategy reports shouldRetry=false, the
// step's final error propagates as a StepError, causing the durable execution
// to fail.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (any, error) {
	_, err := durable.Step(ctx, "doomed-step",
		func(_ durable.StepContext) (any, error) {
			return nil, errors.New("persistent failure")
		},
		durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  6, // 1 initial + 5 retries, then exhausted
			InitialDelay: 1 * time.Second,
			BackoffRate:  1,
			Jitter:       durable.JitterNone,
		})),
	)
	if err != nil {
		return nil, err
	}
	return map[string]string{"status": "completed"}, nil
}

func main() { durable.Start(handler) }
