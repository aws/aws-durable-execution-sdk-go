// Command callback_step_timeout implements conformance requirement 4-8:
// create callback with 5s timeout, run a step, then await (timeout).
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name,
		durable.WithCallbackTimeout(5*time.Second))
	if err != nil {
		return "", err
	}
	_, err = durable.Step(ctx, "notify-external", func(_ durable.StepContext) (string, error) {
		return "notified", nil
	})
	if err != nil {
		return "", err
	}
	return cb.Result()
}

func main() { durable.Start(handler) }
