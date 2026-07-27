// Command parallel-tolerated-failure-percentage demonstrates that a
// Parallel operation fails early when the percentage of branch failures
// strictly exceeds the ToleratedFailurePercentage threshold. Branches that
// had already succeeded when the threshold was exceeded keep their results;
// branches still in flight at that point are abandoned (reported started,
// not counted).
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Output captures the batch outcome, including the preserved results
// from branches that succeeded before the tolerance was exceeded.
type Output struct {
	SuccessCount     int      `json:"successCount"`
	FailureCount     int      `json:"failureCount"`
	TotalCount       int      `json:"totalCount"`
	CompletionReason string   `json:"completionReason"`
	HasFailure       bool     `json:"hasFailure"`
	SuccessResults   []string `json:"successResults"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	// Four branches with two failures. Tolerance is 25%, so after both
	// failures the percentage is (2*100)/4 = 50 which strictly exceeds
	// 25. The batch reports FAILURE_TOLERANCE_EXCEEDED.
	results, err := durable.Parallel(ctx, "failure-percentage-branches", []durable.Branch[string]{
		{Name: "branch-1", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-1", func(_ durable.StepContext) (string, error) {
				return "result-1", nil
			})
		}},
		{Name: "branch-2", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-2", func(_ durable.StepContext) (string, error) {
				return "", fmt.Errorf("branch-2 failed")
			}, durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
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
			}, durable.WithRetry(durable.NewRetryStrategy(durable.RetryConfig{
				MaxAttempts: 1,
			})))
		}},
	}, durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: 25}))
	if err != nil {
		return Output{}, err
	}

	return Output{
		SuccessCount:     results.SuccessCount(),
		FailureCount:     results.FailureCount(),
		TotalCount:       results.TotalCount(),
		CompletionReason: results.Reason.String(),
		HasFailure:       results.HasFailure(),
		SuccessResults:   results.Results(),
	}, nil
}

func main() { durable.Start(handler) }
