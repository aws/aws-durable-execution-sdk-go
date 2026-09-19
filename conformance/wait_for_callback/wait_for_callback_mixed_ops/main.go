// Command wait_for_callback_mixed_ops implements conformance requirement 7-10:
// wait 1s, step, then wait-for-callback.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	if err := durable.Wait(ctx, "", 1*time.Second); err != nil {
		return "", err
	}
	_, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "fixed-data", nil
	})
	if err != nil {
		return "", err
	}
	return durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil })
}

func main() { durable.Start(handler) }
