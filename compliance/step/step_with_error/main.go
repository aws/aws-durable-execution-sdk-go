// Command step_with_error implements conformance requirement 1-19: a step
// that always fails with no retries, failing the execution.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", errors.New("Something went wrong")
	}, durable.WithRetry(durable.NoRetry()))
}

func main() {
	durable.Start(handler)
}
