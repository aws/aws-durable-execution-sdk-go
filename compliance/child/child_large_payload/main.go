// Command child_large_payload implements conformance requirement 3-11: a
// child context where the result exceeds the checkpoint size limit,
// triggering ReplayChildren mode.
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (any, error) {
	executionID := ctx.ExecutionArn()
	result, err := durable.RunInChildContext(ctx, "large-data-processor", func(child durable.Context) (string, error) {
		// Raw stdout write: the SDK's context logger suppresses emissions during
		// replay, and custom runtimes do not get platform-injected execution metadata.
		fmt.Printf("{\"executionArn\":%q,\"message\":%q}\n", executionID, event)

		stepResult, err := durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return strings.Repeat("A", 50*1024), nil
		})
		if err != nil {
			return "", err
		}

		// Result must exceed the 256KB checkpoint threshold to trigger
		// ReplayChildren mode. 50KB × 6 = 300KB > 256KB.
		return strings.Repeat(stepResult, 6), nil
	})
	if err != nil {
		return nil, err
	}

	if err := durable.Wait(ctx, "", 2*time.Second); err != nil {
		return nil, err
	}

	return map[string]any{
		"success":  true,
		"dataSize": len(result),
	}, nil
}

func main() {
	durable.Start(handler)
}
