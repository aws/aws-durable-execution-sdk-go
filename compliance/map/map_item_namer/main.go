// Command map_item_namer implements conformance requirement 9-13: Map with a
// custom item namer assigns per-iteration names.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, items []int) ([]int, error) {
	if len(items) == 0 {
		items = []int{1, 2}
	}
	result, err := durable.Map(ctx, "named-items", items, func(_ durable.Context, item int, _ int) (int, error) {
		return item * 10, nil
	}, durable.WithMaxConcurrency(1), durable.WithItemNamer(func(_ int, i int) string {
		return fmt.Sprintf("item-%d", items[i])
	}))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
