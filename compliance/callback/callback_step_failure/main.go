// Command callback_step_failure implements conformance requirement 4-7:
// create callback, run a step, then await callback result (failure).
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name)
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
