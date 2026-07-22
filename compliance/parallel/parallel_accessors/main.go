// Command parallel_accessors implements conformance requirement 8-20.
package main

import (
	"errors"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (map[string]any, error) {
	result, err := durable.Parallel(ctx, "accessors", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "ok0", nil }},
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("branch failed") }},
		{Func: func(_ durable.Context) (string, error) { return "ok2", nil }},
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.WithToleratedFailureCount(1)))
	if err != nil { return nil, err }
	return map[string]any{
		"hasFailure": result.HasFailure(),
		"successCount": len(result.Succeeded()),
		"failureCount": len(result.Failed()),
		"errorCount": len(result.Errors()),
	}, nil
}

func main() { durable.Start(handler) }
