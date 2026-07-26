// Command map-high-concurrency-invoke demonstrates a Map operation where
// each item invokes another durable function, exercising concurrent
// dispatch at scale with bounded concurrency.
package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Output holds the ordered results from all map item invocations.
type Output struct {
	Results []string `json:"results"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	prefix := os.Getenv("FUNCTION_NAME_PREFIX")
	if prefix == "" {
		prefix = "v2-"
	}
	functionName := prefix + "go-invoke-simple-target"

	const itemCount = 15
	items := make([]int, itemCount)
	for i := range items {
		items[i] = i
	}

	results, err := durable.Map(ctx, "process-items", items,
		func(ctx durable.Context, item int, _ int) (string, error) {
			return durable.Invoke[string](ctx, fmt.Sprintf("invoke-item-%d", item),
				functionName, fmt.Sprintf("payload-%d", item))
		},
		durable.WithMaxConcurrency(5),
	)
	if err != nil {
		return Output{}, err
	}

	return Output{Results: results.Results()}, nil
}

func main() { durable.Start(handler) }
