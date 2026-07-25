// Command map-tolerated-failure-percentage demonstrates a Map operation
// with ToleratedFailurePercentage completion config that fails the batch
// once the failure percentage strictly exceeds the threshold.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Output struct {
	SuccessCount     int      `json:"successCount"`
	FailureCount     int      `json:"failureCount"`
	TotalCount       int      `json:"totalCount"`
	CompletionReason string   `json:"completionReason"`
	HasFailure       bool     `json:"hasFailure"`
	Results          []string `json:"results"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	results, err := durable.Map(ctx, "percentage-items", items,
		func(ctx durable.Context, item int, index int) (string, error) {
			return durable.Step(ctx, fmt.Sprintf("process-%d", index),
				func(_ durable.StepContext) (string, error) {
					// Items at indices 2, 5, 8 fail (every index where index%3==2).
					if index%3 == 2 {
						return "", fmt.Errorf("processing failed for item %d", item)
					}
					return fmt.Sprintf("Item %d processed", item), nil
				},
				durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
					MaxAttempts: 1,
				})),
			)
		},
		durable.WithCompletion(durable.CompletionConfig{
			ToleratedFailurePercentage: 25,
		}),
		durable.WithMaxConcurrency(1),
	)
	if err != nil {
		return Output{}, err
	}

	_ = durable.Wait(ctx, "wait", 1*time.Second)

	return Output{
		SuccessCount:     results.SuccessCount(),
		FailureCount:     results.FailureCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
		HasFailure:       results.HasFailure(),
		Results:          results.Results(),
	}, nil
}

func main() { durable.Start(handler) }
