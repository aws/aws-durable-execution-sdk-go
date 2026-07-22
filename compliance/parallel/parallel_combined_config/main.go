// Command parallel_combined_config implements conformance requirement 8-18.
package main

import (
	"errors"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	cfg := durable.WithToleratedFailureCount(1)
	cfg.MinSuccessful = 3
	result, err := durable.Parallel(ctx, "combined", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("fail0") }},
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("fail1") }},
		{Func: func(_ durable.Context) (string, error) { return "ok2", nil }},
		{Func: func(_ durable.Context) (string, error) { return "ok3", nil }},
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(cfg))
	if err != nil { return nil, err }
	return map[string]any{
		"completionReason": result.Reason.String(),
		"successCount": result.SuccessCount(),
		"failureCount": result.FailureCount(),
		"totalCount": result.TotalCount(),
	}, nil
}

func main() { durable.Start(handler) }
