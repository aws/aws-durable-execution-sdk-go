// Command parallel_all_fail implements conformance requirement 8-16.
package main

import (
	"errors"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	result, err := durable.Parallel(ctx, "all-fail", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("fail0") }},
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("fail1") }},
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("fail2") }},
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.WithToleratedFailureCount(3)))
	if err != nil { return nil, err }
	return map[string]any{
		"completionReason": result.Reason.String(),
		"status": result.Status(),
		"successCount": result.SuccessCount(),
		"failureCount": result.FailureCount(),
		"totalCount": result.TotalCount(),
	}, nil
}

func main() { durable.Start(handler) }
