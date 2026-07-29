// Command parallel-failure-threshold-percentage demonstrates a Parallel
// operation with ToleratedFailurePercentage where the handler propagates
// the batch failure as an error, causing the execution to reach a FAILED
// terminal state.
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
	// Four branches with two failures. Tolerance is 25%, so after both
	// failures the percentage is (2*100)/4 = 50 which exceeds 25.
	results, err := durable.Parallel(ctx, "failure-percentage-branches", []durable.Branch[string]{
		{Name: "branch-1", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-1", func(_ durable.StepContext) (string, error) {
				return "result-1", nil
			})
		}},
		{Name: "branch-2", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-2", func(_ durable.StepContext) (string, error) {
				return "", fmt.Errorf("branch-2 failed")
			}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
				MaxAttempts: 1,
			})))
		}},
		{Name: "branch-3", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-3", func(_ durable.StepContext) (string, error) {
				return "result-3", nil
			})
		}},
		{Name: "branch-4", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-4", func(_ durable.StepContext) (string, error) {
				return "", fmt.Errorf("branch-4 failed")
			}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
				MaxAttempts: 1,
			})))
		}},
	}, durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: 25}),
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
