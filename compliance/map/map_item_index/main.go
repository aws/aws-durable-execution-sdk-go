// Command map_item_index implements conformance requirement 9-3: Map function
// receives both the item and its zero-based index and uses both.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, items []int) ([]int, error) {
	if len(items) == 0 {
		items = []int{10, 20, 30}
	}
	result, err := durable.Map(ctx, "indexed", items, func(_ durable.Context, item int, index int) (int, error) {
		return item + index, nil
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
