// Command child_wait_replay implements conformance requirement 3-13: a
// child context containing only a wait operation, followed by a step
// outside the child.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	_, err := durable.RunInChildContext(ctx, "wait-child", func(child durable.Context) (string, error) {
		if err := durable.Wait(child, "", 1*time.Second); err != nil {
			return "", err
		}
		return event, nil
	})
	if err != nil {
		return "", err
	}

	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return event, nil
	})
}

func main() {
	durable.Start(handler)
}
