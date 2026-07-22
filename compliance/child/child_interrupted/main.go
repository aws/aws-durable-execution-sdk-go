// Command child_interrupted implements conformance requirement 3-12: a
// child context where the step crashes the Lambda process on the first
// attempt and succeeds on the second.
package main

import (
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/attempts"
)

func handler(ctx durable.Context, event string) (string, error) {
	executionID := ctx.ExecutionArn()

	return durable.RunInChildContext(ctx, "", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(sc durable.StepContext) (string, error) {
			count, err := attempts.Increment(sc, executionID)
			if err != nil {
				return "", err
			}
			if count < 2 {
				time.Sleep(time.Second) // allow checkpoint to flush
				os.Exit(1)
			}
			return event, nil
		}, durable.WithRetry(durable.NoRetry()))
	})
}

func main() {
	durable.Start(handler)
}
