// Command map-high-concurrency-invoke demonstrates a Map operation where
// each item invokes another durable function, exercising concurrent
// dispatch at scale with bounded concurrency.
package main

import (
	"encoding/json"
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
	functionName := prefix + "go-invoke-simple-target:$LATEST"

	const itemCount = 15
	items := make([]int, itemCount)
	for i := range items {
		items[i] = i
	}

	results, err := durable.Map(ctx, "process-items", items,
		func(ctx durable.Context, item int, _ int) (string, error) {
			raw, err := durable.Invoke[json.RawMessage](ctx, fmt.Sprintf("invoke-item-%d", item),
				functionName, map[string]string{"message": fmt.Sprintf("payload-%d", item)})
			if err != nil {
				return "", err
			}
			// Try to decode as a plain JSON string first; if that fails,
			// return the raw JSON representation (e.g. for struct responses).
			var s string
			if json.Unmarshal(raw, &s) == nil {
				return s, nil
			}
			return string(raw), nil
		},
		durable.WithMaxConcurrency(5),
	)
	if err != nil {
		return Output{}, err
	}

	return Output{Results: results.Results()}, nil
}

func main() { durable.Start(handler) }
