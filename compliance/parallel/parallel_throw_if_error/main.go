// Command parallel_throw_if_error implements conformance requirement 8-7.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "throwing", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "", errors.New("branch failed") }},
		{Func: func(_ durable.Context) (string, error) { return "never", nil }},
	}, durable.WithMaxConcurrency(1), durable.WithCompletion(durable.WithToleratedFailureCount(0)))
	if err != nil {
		return nil, err
	}
	if throwErr := result.ThrowIfError(); throwErr != nil {
		return nil, throwErr
	}
	return result.Results(), nil
}

func main() { durable.Start(handler) }
