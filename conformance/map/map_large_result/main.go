// Command map_large_result implements conformance requirement 9-16: Map whose
// combined item results exceed the large-result threshold checkpoints with
// replay-children.
package main

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (map[string]int, error) {
	items := []int{0, 1, 2, 3}
	result, err := durable.Map(ctx, "large", items, func(_ durable.Context, _ int, _ int) (string, error) {
		// Each item returns ~70KB so aggregate > 256KB.
		return strings.Repeat("X", 70*1024), nil
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return map[string]int{
		"successCount": result.SuccessCount(),
		"totalCount":   result.TotalCount(),
	}, nil
}

func main() {
	durable.Start(handler)
}
