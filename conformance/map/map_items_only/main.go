// Command map_items_only implements conformance requirement 9-2: Map invoked
// with the items-only form (no name argument), each item returns directly.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, items []int) ([]int, error) {
	if len(items) == 0 {
		items = []int{1, 2}
	}
	result, err := durable.Map(ctx, "", items, func(_ durable.Context, item int, _ int) (int, error) {
		return item * 2, nil
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
