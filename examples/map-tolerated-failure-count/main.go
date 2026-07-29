// Command map-tolerated-failure-count demonstrates a Map operation with
// ToleratedFailureCount completion config that allows up to N failures
// before failing the batch.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Output struct {
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
	CompletionReason string `json:"completionReason"`
	HasFailure       bool   `json:"hasFailure"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []int{1, 2, 3, 4, 5}

	results, err := durable.Map(ctx, "failure-count-items", items,
		func(ctx durable.Context, item int, index int) (string, error) {
			return durable.Step(ctx, fmt.Sprintf("process-%d", index),
				func(_ durable.StepContext) (string, error) {
					// Items 2 and 4 will fail.
					if item == 2 || item == 4 {
						return "", fmt.Errorf("processing failed for item %d", item)
					}
					return fmt.Sprintf("Item %d processed", item), nil
				},
				durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
					MaxAttempts: 1,
				})),
			)
		},
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(2)}),
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
	}, nil
}

func main() { durable.Start(handler) }
