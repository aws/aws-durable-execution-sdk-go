// Command step_default_retry implements conformance requirement 1-13: a
// step that fails on the first two attempts and succeeds on the third,
// using the SDK's default retry strategy (no explicit configuration).
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/attempts"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	executionID := ctx.ExecutionArn()

	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		count, err := attempts.Increment(sc, executionID)
		if err != nil {
			return "", err
		}
		if count < 3 {
			return "", fmt.Errorf("Attempt %d failed", count)
		}
		return "recovered", nil
	})
}

func main() {
	durable.Start(handler)
}
