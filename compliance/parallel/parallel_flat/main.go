// Command parallel_flat implements conformance requirement 8-12.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "flat", []durable.Branch[string]{
		{Func: func(childCtx durable.Context) (string, error) {
			return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) { return "fa", nil })
		}},
		{Func: func(childCtx durable.Context) (string, error) {
			return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) { return "fb", nil })
		}},
	}, durable.WithMaxConcurrency(1), durable.WithNesting(durable.NestingFlat))
	if err != nil { return nil, err }
	return result.Results(), nil
}

func main() { durable.Start(handler) }
