// Command step_nested implements conformance requirement 1-3: two
// sequential steps where the second uses the result of the first.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	first, err := durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "first", nil
	})
	if err != nil {
		return "", err
	}
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return first + "_second", nil
	})
}

func main() {
	durable.Start(handler)
}
