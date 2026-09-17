// Command child_step_wait_after implements conformance requirement 3-18: a
// child context containing a step and wait, then a step and wait at the
// top level after the child.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	_, err := durable.RunInChildContext(ctx, "step-wait-child", func(child durable.Context) (string, error) {
		_, err := durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return event, nil
		})
		if err != nil {
			return "", err
		}

		if err := durable.Wait(child, "", 2*time.Second); err != nil {
			return "", err
		}

		return event, nil
	})
	if err != nil {
		return "", err
	}

	result, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return event, nil
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
