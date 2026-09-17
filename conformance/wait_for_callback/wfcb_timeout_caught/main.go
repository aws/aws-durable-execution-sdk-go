// Command wfcb_timeout_caught implements conformance requirement 7-14:
// wait-for-callback timeout caught, returns "timed-out-handled".
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	result, err := durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil },
		durable.WithCallbackTimeout(3*time.Second))
	if err != nil {
		return "timed-out-handled", nil
	}
	return result, nil
}

func main() { durable.Start(handler) }
