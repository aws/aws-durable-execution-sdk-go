// Command invoke_then_step implements conformance requirement 5-12: an
// invoke result feeds a subsequent step.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event any) (string, error) {
	invokeResult, err := durable.Invoke[string](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event)
	if err != nil {
		return "", err
	}
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "processed: " + invokeResult, nil
	})
}

func main() {
	durable.Start(handler)
}
