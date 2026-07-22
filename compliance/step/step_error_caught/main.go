// Command step_error_caught implements conformance requirement 1-20: a
// step fails, user code catches the error, and execution continues with a
// fallback step.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	_, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "", errors.New("Something went wrong")
	}, durable.WithRetry(durable.NoRetry()))
	var stepErr *durable.StepError
	if !errors.As(err, &stepErr) {
		return "", err
	}

	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "fallback_result", nil
	})
}

func main() {
	durable.Start(handler)
}
