// Command parallel_heterogeneous implements conformance requirement 8-4.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]any, error) {
	result, err := durable.Parallel(ctx, "hetero", []durable.Branch[any]{
		{Func: func(_ durable.Context) (any, error) { return "hello", nil }},
		{Func: func(_ durable.Context) (any, error) { return 42, nil }},
		{Func: func(_ durable.Context) (any, error) { return map[string]string{"k": "v"}, nil }},
	}, durable.WithMaxConcurrency(1))
	if err != nil { return nil, err }
	return result.Results(), nil
}

func main() { durable.Start(handler) }
