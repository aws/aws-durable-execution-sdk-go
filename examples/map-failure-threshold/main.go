// Command map-failure-threshold demonstrates a Map operation where the
// number of failures exceeds the tolerated failure count, causing the
// batch to fail early with FAILURE_TOLERANCE_EXCEEDED.
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Output struct {
	CompletionReason string `json:"completionReason"`
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []int{1, 2, 3, 4, 5}

	results, err := durable.Map(ctx, "failure-threshold-items", items,
		func(ctx durable.Context, item int, index int) (int, error) {
			return durable.Step(ctx, fmt.Sprintf("process-%d", index),
				func(_ durable.StepContext) (int, error) {
					// Items 1-3 fail, exceeding the tolerance of 2.
					if item <= 3 {
						return 0, fmt.Errorf("item %d failed", item)
					}
					return item * 2, nil
				},
				durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
					MaxAttempts: 1,
				})),
			)
		},
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(2)}),
	)
	// Failed items are reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error is
	// an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return Output{}, err
	}

	_ = durable.Wait(ctx, "wait", 1*time.Second)

	return Output{
		CompletionReason: results.Reason.String(),
		SuccessCount:     results.SuccessCount(),
		FailureCount:     results.FailureCount(),
		TotalCount:       results.TotalCount(),
	}, nil
}

func main() { durable.Start(handler) }
