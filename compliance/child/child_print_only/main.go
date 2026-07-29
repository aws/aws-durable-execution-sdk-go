// Command child_print_only implements conformance requirement 3-17: a
// child context that only prints to stdout and returns (no durable
// operations inside), followed by a wait that causes a replay.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	executionID := ctx.ExecutionArn()
	result, err := durable.RunInChildContext(ctx, "print-child", func(child durable.Context) (string, error) {
		// Raw stdout write: the SDK's context logger suppresses emissions during
		// replay, and custom runtimes do not get platform-injected execution metadata.
		fmt.Printf("{\"executionArn\":%q,\"message\":%q}\n", executionID, event)
		return event, nil
	})
	if err != nil {
		return "", err
	}

	if err := durable.Wait(ctx, "", 1*time.Second); err != nil {
		return "", err
	}

	return result, nil
}

func main() {
	durable.Start(handler)
}
