// Command parallel-failure-threshold-count demonstrates a Parallel
// operation with ToleratedFailureCount=0 (fail-fast). The first branch
// failure immediately exceeds the threshold, and the handler propagates
// the failure as an error causing a FAILED terminal state.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Output struct {
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	TotalCount       int    `json:"totalCount"`
	CompletionReason string `json:"completionReason"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	results, err := durable.Parallel(ctx, "fail-fast-branches", []durable.Branch[string]{
		{Name: "branch-1", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-1", func(_ durable.StepContext) (string, error) {
				return "Branch 1 success", nil
			})
		}},
		{Name: "branch-2", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-2", func(_ durable.StepContext) (string, error) {
				return "", fmt.Errorf("branch 2 failed")
			}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
				MaxAttempts: 1,
			})))
		}},
		{Name: "branch-3", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-3", func(_ durable.StepContext) (string, error) {
				return "Branch 3 success", nil
			})
		}},
	}, durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(0)}),
		durable.WithMaxConcurrency(1))
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
