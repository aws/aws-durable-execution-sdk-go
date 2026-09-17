// Command map_basic implements conformance requirement 9-1: Map basic
// applies a function to each item, each item runs a single step, all succeed.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, items []string) ([]string, error) {
	if len(items) == 0 {
		items = []string{"World", "Kiro"}
	}
	result, err := durable.Map(ctx, "map", items, func(childCtx durable.Context, item string, _ int) (string, error) {
		return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) {
			return "Hello, " + item + "!", nil
		})
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
