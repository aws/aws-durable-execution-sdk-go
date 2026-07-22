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
	result, err := durable.RunInChildContext(ctx, "large-data-processor", func(child durable.Context) (string, error) {
		fmt.Println(event)

		stepResult, err := durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return strings.Repeat("A", 50*1024), nil
		})
		if err != nil {
			return "", err
		}

		// Build a large result (>256KB) from the small step result.
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
