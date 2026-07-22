// Command map-basic demonstrates a Map operation that processes an array
// of items concurrently with bounded concurrency.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]int, error) {
	items := []int{1, 2, 3, 4, 5}

	results, err := durable.Map(ctx, "map", items,
		func(ctx durable.Context, item int, _ int) (int, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (int, error) {
				return item * 2, nil
			})
		},
		durable.WithMaxConcurrency(2),
	)
	if err != nil {
		return nil, err
	}

	// Wait ensures results are stable across replay boundaries.
	_ = durable.Wait(ctx, "wait", 1*time.Second)

	return results.Results(), nil
}

func main() { durable.Start(handler) }
