// Command map_flat implements conformance requirement 9-12: Map with FLAT
// nesting executes items in virtual contexts, omitting per-iteration events.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) ([]string, error) {
	items := []string{"fa", "fb"}
	result, err := durable.Map(ctx, "flat", items, func(childCtx durable.Context, item string, _ int) (string, error) {
		return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) {
			return item, nil
		})
	}, durable.WithMaxConcurrency(1), durable.WithNesting(durable.NestingFlat))
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
}

func main() {
	durable.Start(handler)
}
