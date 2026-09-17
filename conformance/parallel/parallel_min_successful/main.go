// Command parallel_min_successful implements conformance requirement 8-8.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	result, err := durable.Parallel(ctx, "min-successful", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "a", nil }},
		{Func: func(_ durable.Context) (string, error) { return "b", nil }},
		{Func: func(_ durable.Context) (string, error) { return "c", nil }},
		{Func: func(_ durable.Context) (string, error) { return "d", nil }},
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{MinSuccessful: 2}))
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"completionReason": result.Reason.String(),
		"successCount":     result.SuccessCount(),
		"totalCount":       result.TotalCount(),
	}, nil
}

func main() { durable.Start(handler) }
