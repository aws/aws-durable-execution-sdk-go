// Command step_then_invoke implements conformance requirement 5-11: a step
// computes a payload, then an invoke sends it to the target.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	stepResult, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "step result", nil
	})
	if err != nil {
		return "", err
	}
	return durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), stepResult)
}

func main() {
	durable.Start(handler)
}
