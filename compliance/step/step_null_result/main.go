// Command step_null_result implements conformance requirement 1-5: a step
// that returns a null result.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (any, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (any, error) {
		return nil, nil
	})
}

func main() {
	durable.Start(handler)
}
