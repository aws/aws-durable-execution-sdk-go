// Command wait_for_callback_timeout implements conformance requirement 7-5:
// wait-for-callback with 3s timeout, no external completion.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	return durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil },
		durable.WithCallbackTimeout(3*time.Second))
}

func main() { durable.Start(handler) }
