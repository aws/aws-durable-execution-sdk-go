// Command parallel_branches_only implements conformance requirement 8-2.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "alpha", nil }},
		{Func: func(_ durable.Context) (string, error) { return "beta", nil }},
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() { durable.Start(handler) }
