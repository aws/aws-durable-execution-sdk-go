// Command map_then_wait implements conformance requirement 9-17: Suspension
// after a successful map; on replay the completed map is skipped.
package main

import (
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"a", "b"}
	result, err := durable.Map(ctx, "then-wait", items, func(_ durable.Context, item string, _ int) (string, error) {
		return strings.ToUpper(item), nil
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	if err := durable.Wait(ctx, "", 1*time.Second); err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
