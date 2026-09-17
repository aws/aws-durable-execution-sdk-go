// Conformance 1-17: AtMostOnce no-retry, Lambda crash.
package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	executionID := ctx.ExecutionArn()
	return durable.Step(ctx, "at_most_once_flaky_step", func(sc durable.StepContext) (string, error) {
		// Raw stdout write: the SDK's context logger suppresses emissions during
		// replay, and custom runtimes do not get platform-injected execution metadata.
		fmt.Printf("{\"executionArn\":%q,\"message\":%q}\n", executionID, event)
		os.Exit(1)
		return "unreachable", nil
	}, durable.WithSemantics(durable.AtMostOncePerRetry), durable.WithRetry(durable.NoRetry()))
}

func main() {
	durable.Start(handler)
}
