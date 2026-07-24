// Command parallel_bad_concurrency implements conformance requirement 8-19.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "bad-concurrency", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "a", nil }},
		{Func: func(_ durable.Context) (string, error) { return "b", nil }},
	}, durable.WithMaxConcurrency(0))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() { durable.Start(handler) }
