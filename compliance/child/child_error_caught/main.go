// Command child_error_caught implements conformance requirement 3-5: a
// child context where the inner step fails, but the error is caught and
// execution continues with a recovery step.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	_, err := durable.RunInChildContext(ctx, "", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return "", errors.New("Child step failed")
		}, durable.WithRetry(durable.NoRetry()))
	})

	// Catch the ChildContextError and continue.
	var childErr *durable.ChildContextError
	if err != nil && !errors.As(err, &childErr) {
		return "", err
	}

	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return event, nil
	})
}

func main() {
	durable.Start(handler)
}
