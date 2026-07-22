// Command child_replay implements conformance requirement 3-9: a child
// context followed by a wait; on replay the child returns its cached
// result without re-executing.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	result, err := durable.RunInChildContext(ctx, "", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return event, nil
		})
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
