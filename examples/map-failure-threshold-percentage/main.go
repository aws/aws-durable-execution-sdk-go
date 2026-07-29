// Command map-failure-threshold-percentage demonstrates a Map operation
// with ToleratedFailurePercentage where the handler propagates the batch
// failure as an error, causing the execution to reach a FAILED terminal
// state.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Output struct {
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
	CompletionReason string `json:"completionReason"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []int{0, 1, 2, 3, 4}

	results, err := durable.Map(ctx, "percentage-items", items,
		func(ctx durable.Context, item int, index int) (string, error) {
			return durable.Step(ctx, fmt.Sprintf("process-%d", index),
				func(_ durable.StepContext) (string, error) {
					// Items at indices 1, 3 fail. With 5 items and 25% tolerance,
					// (2*100)/5 = 40% > 25% triggers FAILURE_TOLERANCE_EXCEEDED.
					if index == 1 || index == 3 {
						return "", fmt.Errorf("item %d failed", item)
					}
					return fmt.Sprintf("Item %d processed", item), nil
				},
				durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
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

	// Propagate the failure when tolerance is exceeded.
	if results.Reason == durable.CompletionFailureToleranceExceeded {
		return Output{}, fmt.Errorf("batch failed: tolerance exceeded (failures=%d, total=%d)",
			results.FailureCount(), results.TotalCount())
	}

	return Output{
		SuccessCount:     results.SuccessCount(),
		FailureCount:     results.FailureCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
	}, nil
}

func main() { durable.Start(handler) }
