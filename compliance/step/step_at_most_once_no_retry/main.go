// Conformance 1-17: AtMostOnce no-retry, Lambda crash.
package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	return durable.Step(ctx, "at_most_once_flaky_step", func(_ durable.StepContext) (string, error) {
		fmt.Println(event)
		os.Exit(1)
		return "unreachable", nil
	}, durable.WithSemantics(durable.AtMostOncePerRetry), durable.WithRetry(durable.NoRetry()))
}

func main() {
	durable.Start(handler)
}
