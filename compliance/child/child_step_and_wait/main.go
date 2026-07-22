// Command child_step_and_wait implements conformance requirement 3-10: a
// child context containing a step followed by a wait operation.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	return durable.RunInChildContext(ctx, "mixed-ops", func(child durable.Context) (string, error) {
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
}

func main() {
	durable.Start(handler)
}
