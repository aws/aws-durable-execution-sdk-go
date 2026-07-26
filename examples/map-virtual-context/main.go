// Command map-virtual-context demonstrates durable.Map with
// durable.WithNesting(durable.NestingFlat), which runs each item in a
// virtual context. Operations inside items are checkpointed directly under
// the parent batch context — no per-item ContextStarted/Succeeded events
// are emitted, reducing checkpoint overhead at the cost of less granular
// replay.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

// Output holds the handler's result.
type Output struct {
	ProcessedItems []int `json:"processedItems"`
	TotalCount     int   `json:"totalCount"`
	SuccessCount   int   `json:"successCount"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []int{1, 2, 3, 4, 5}

	results, err := durable.Map(ctx, "process-items", items,
		func(ctx durable.Context, item int, _ int) (int, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (int, error) {
				return item * 2, nil
			})
		},
		durable.WithNesting(durable.NestingFlat),
		durable.WithMaxConcurrency(1),
	)
	if err != nil {
		return Output{}, err
	}

	return Output{
		ProcessedItems: results.Results(),
		TotalCount:     results.TotalCount(),
		SuccessCount:   results.SuccessCount(),
	}, nil
}

func main() { durable.Start(handler) }
