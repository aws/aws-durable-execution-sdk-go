// Command parallel_named_branches implements conformance requirement 8-3.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "named", []durable.Branch[string]{
		{Name: "first", Func: func(_ durable.Context) (string, error) { return "one", nil }},
		{Name: "second", Func: func(_ durable.Context) (string, error) { return "two", nil }},
	}, durable.WithMaxConcurrency(1))
	if err != nil { return nil, err }
	return result.Results(), nil
}

func main() { durable.Start(handler) }
