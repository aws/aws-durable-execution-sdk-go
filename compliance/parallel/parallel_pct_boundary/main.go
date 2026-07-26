// Command parallel_pct_boundary implements conformance requirement 8-22.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	result, err := durable.Parallel(ctx, "pct-boundary", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("fail") }},
		{Func: func(_ durable.Context) (string, error) { return "ok1", nil }},
		{Func: func(_ durable.Context) (string, error) { return "ok2", nil }},
		{Func: func(_ durable.Context) (string, error) { return "ok3", nil }},
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: 25}))
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"completionReason": result.Reason.String(),
		"status":           result.Status(),
		"successCount":     result.SuccessCount(),
		"failureCount":     result.FailureCount(),
		"totalCount":       result.TotalCount(),
	}, nil
}

func main() { durable.Start(handler) }
