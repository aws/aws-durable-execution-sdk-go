// Command parallel_throw_if_error implements conformance requirement 8-7.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "throwing", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("branch failed") }},
		{Func: func(_ durable.Context) (string, error) { return "never", nil }},
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.CompletionConfig{ToleratedFailureCount: aws.Int(0)}))
	// The item failure is returned as a *durable.BatchError; returning it
	// fails the execution.
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() { durable.Start(handler) }
