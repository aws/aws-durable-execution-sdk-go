// Command wait_for_callback_submitter_retry_exhaustion implements conformance requirement 7-7:
// wait-for-callback whose submitter always throws, retry exhaustion.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	return durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error {
			return errors.New("submitter failure")
		},
		durable.WithSubmitterRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
			MaxAttempts:  2,
			InitialDelay: 1 * time.Second,
			MaxDelay:     1 * time.Second,
		})))
}

func main() { durable.Start(handler) }
