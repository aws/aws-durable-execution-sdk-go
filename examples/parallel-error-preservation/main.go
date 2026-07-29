// Command parallel-error-preservation demonstrates that errors in parallel
// branches preserve their original type and message through BatchResult.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type ErrorInfo struct {
	Message string `json:"message"`
	IsStep  bool   `json:"isStepError"`
}

type Output struct {
	Success     []string    `json:"success"`
	Errors      []ErrorInfo `json:"errors"`
	TotalErrors int         `json:"totalErrors"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	results, err := durable.Parallel(ctx, "parallel-with-errors", []durable.Branch[string]{
		{Name: "success-task", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "success-task", func(_ durable.StepContext) (string, error) {
				return "task completed successfully", nil
			})
		}},
		{Name: "failing-task", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "failing-task", func(_ durable.StepContext) (string, error) {
				return "", fmt.Errorf("custom error message")
			}, durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
				MaxAttempts: 1,
			})))
		}},
	})
	if err != nil {
		return Output{}, err
	}

	var errorInfos []ErrorInfo
	for _, e := range results.Errors() {
		var stepErr *durable.StepError
		errorInfos = append(errorInfos, ErrorInfo{
			Message: e.Error(),
			IsStep:  errors.As(e, &stepErr),
		})
	}

	var successes []string
	for _, item := range results.Succeeded() {
		successes = append(successes, item.Result)
	}

	return Output{
		Success:     successes,
		Errors:      errorInfos,
		TotalErrors: len(results.Errors()),
	}, nil
}

func main() { durable.Start(handler) }
