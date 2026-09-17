// Command step_with_name implements conformance requirement 1-2: a single
// step with an explicit name parameter.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "custom_step_name", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler)
}
