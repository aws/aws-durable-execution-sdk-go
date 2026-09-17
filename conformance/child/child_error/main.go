// Command child_error implements conformance requirement 3-4: a child
// context where the inner step throws an error with no retry, causing the
// execution to fail.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.RunInChildContext(ctx, "failing-child", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return "", errors.New("Child step failed")
		}, durable.WithRetry(durable.NoRetry()))
	})
}

func main() {
	durable.Start(handler)
}
