// Command map_concurrent implements conformance requirement 9-11: Map with
// max-concurrency > 1 preserves index-ordered results.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"r0", "r1", "r2"}
	result, err := durable.Map(ctx, "concurrent", items, func(_ durable.Context, item string, _ int) (string, error) {
		return item, nil
	}, durable.WithMaxConcurrency(2))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
