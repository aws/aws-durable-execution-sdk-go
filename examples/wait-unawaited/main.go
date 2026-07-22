// Command wait-unawaited demonstrates scheduling a wait operation via
// [durable.WaitAsync] without blocking on the result. The function
// completes immediately while the wait is scheduled in the background.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	// Schedule a wait without blocking on it. The function returns its
	// result immediately; the scheduled wait proceeds independently.
	_ = durable.WaitAsync(ctx, "background-wait", 5*time.Second)

	// A brief step to ensure the async operation is dispatched before
	// the handler returns.
	return durable.Step(ctx, "complete", func(_ durable.StepContext) (string, error) {
		return "result", nil
	})
}

func main() { durable.Start(handler) }
