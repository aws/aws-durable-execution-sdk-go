// Command map-min-successful demonstrates a Map operation with
// MinSuccessful completion config that completes early once enough
// items succeed.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Output struct {
	SuccessCount     int      `json:"successCount"`
	TotalCount       int      `json:"totalCount"`
	CompletionReason string   `json:"completionReason"`
	Results          []string `json:"results"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []int{1, 2, 3, 4, 5}

	results, err := durable.Map(ctx, "min-successful-items", items,
		func(ctx durable.Context, item int, index int) (string, error) {
			return durable.Step(ctx, fmt.Sprintf("process-%d", index),
				func(_ durable.StepContext) (string, error) {
					time.Sleep(time.Duration(100*item) * time.Millisecond)
					return fmt.Sprintf("Item %d processed", item), nil
				})
		},
		durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}),
		durable.WithItemNamer(func(i int) string { return fmt.Sprintf("process-%d", i) }),
	)
	if err != nil {
		return Output{}, err
	}

	_ = durable.Wait(ctx, "wait", 1*time.Second)

	return Output{
		SuccessCount:     results.SuccessCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
		Results:          results.Results(),
	}, nil
}

func main() { durable.Start(handler) }
