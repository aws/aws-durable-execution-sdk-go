// Conformance 3-11: Child context with large payload (ReplayChildren mode).
package main

import (
	"fmt"
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, event string) (string, error) {
	return durable.RunInChildContext(ctx, "large-child", func(child durable.Context) (string, error) {
		fmt.Println(event)
		stepResult, err := durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return strings.Repeat("A", 50*1024), nil
		})
		if err != nil {
			return "", err
		}
		return stepResult[:10], nil
	})
}

func main() {
	durable.Start(handler)
}
