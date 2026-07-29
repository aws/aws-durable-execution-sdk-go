// Command parallel-tolerated-failure demonstrates a Parallel operation
// with ToleratedFailureCount completion config that allows up to N
// failures before failing the batch.
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
	results, err := durable.Parallel(ctx, "failure-count-branches", []durable.Branch[string]{
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
		{Name: "branch-4", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-4", func(_ durable.StepContext) (string, error) {
				return "", fmt.Errorf("branch 4 failed")
			}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
				MaxAttempts: 1,
			})))
		}},
		{Name: "branch-5", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "branch-5", func(_ durable.StepContext) (string, error) {
				return "Branch 5 success", nil
			})
		}},
	}, durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(2)}))
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
