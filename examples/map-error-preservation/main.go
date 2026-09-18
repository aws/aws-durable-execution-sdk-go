// Command map-error-preservation demonstrates that errors thrown in map
// items preserve their original type and message through the BatchResult.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Item struct {
	ID         int    `json:"id"`
	ShouldFail bool   `json:"shouldFail"`
	ErrorType  string `json:"errorType,omitempty"`
}

type ErrorInfo struct {
	Message string `json:"message"`
	IsStep  bool   `json:"isStepError"`
}

type Output struct {
	Success     []string    `json:"success"`
	Errors      []ErrorInfo `json:"errors"`
	TotalErrors int         `json:"totalErrors"`
	TotalOK     int         `json:"totalSuccess"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	items := []Item{
		{ID: 1, ShouldFail: false},
		{ID: 2, ShouldFail: true, ErrorType: "custom"},
		{ID: 3, ShouldFail: false},
	}

	results, err := durable.Map(ctx, "map-with-errors", items,
		func(ctx durable.Context, item Item, index int) (string, error) {
			return durable.Step(ctx, fmt.Sprintf("process-item-%d", index),
				func(_ durable.StepContext) (string, error) {
					if item.ShouldFail {
						return "", fmt.Errorf("custom error for item %d", item.ID)
					}
					return fmt.Sprintf("Processed item %d", item.ID), nil
				},
				durable.WithRetry(durable.MustNewRetryStrategy(durable.RetryConfig{
					MaxAttempts: 1,
				})),
			)
		},
		// Tolerate the one failing item so every item runs; the default
		// completion policy would stop the batch at the first failure.
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(1)}),
	)
	// Failed items are reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error is
	// an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return Output{}, err
	}

	// Examine errors preserving type info.
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
		TotalOK:     results.SuccessCount(),
	}, nil
}

func main() { durable.Start(handler) }
