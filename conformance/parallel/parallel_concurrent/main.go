// Command parallel_concurrent implements conformance requirement 8-11.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "concurrent", []durable.Branch[string]{
		{Func: func(_ durable.Context) (string, error) { return "r0", nil }},
		{Func: func(_ durable.Context) (string, error) { return "r1", nil }},
		{Func: func(_ durable.Context) (string, error) { return "r2", nil }},
	}, durable.WithMaxConcurrency(2))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() { durable.Start(handler) }
