// Command parallel_nested implements conformance requirement 8-21.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([][]string, error) {
	result, err := durable.Parallel(ctx, "outer", []durable.Branch[[]string]{
		{Func: func(outerCtx durable.Context) ([]string, error) {
			inner, err := durable.Parallel(outerCtx, "inner", []durable.Branch[string]{
				{Func: func(innerCtx durable.Context) (string, error) {
					return durable.Step(innerCtx, "", func(_ durable.StepContext) (string, error) { return "i1", nil })
				}},
				{Func: func(innerCtx durable.Context) (string, error) {
					return durable.Step(innerCtx, "", func(_ durable.StepContext) (string, error) { return "i2", nil })
				}},
			}, durable.WithMaxConcurrency(1))
			if err != nil { return nil, err }
			return inner.Results(), nil
		}},
	}, durable.WithMaxConcurrency(1))
	if err != nil { return nil, err }
	return result.Results(), nil
}

func main() { durable.Start(handler) }
