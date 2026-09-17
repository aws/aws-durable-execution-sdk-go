// Command parallel_basic implements conformance requirement 8-1.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "parallel", []durable.Branch[string]{
		{Func: func(childCtx durable.Context) (string, error) {
			return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) { return "task-1", nil })
		}},
		{Func: func(childCtx durable.Context) (string, error) {
			return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) { return "task-2", nil })
		}},
	}, durable.WithMaxConcurrency(1))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() { durable.Start(handler) }
