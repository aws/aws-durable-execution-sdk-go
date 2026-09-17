// Command step_and_wait_replay implements conformance requirement 1-8: a
// step followed by a wait, testing that replay skips the completed step.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	result, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "computed", nil
	})
	if err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return "", err
	}
	return result, nil
}

func main() {
	durable.Start(handler)
}
