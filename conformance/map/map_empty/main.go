// Command map_empty implements conformance requirement 9-4: Map invoked with
// an empty items list completes immediately with no iterations.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, items []string) ([]string, error) {
	result, err := durable.Map(ctx, "empty", items, func(_ durable.Context, item string, _ int) (string, error) {
		return item, nil
	})
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
