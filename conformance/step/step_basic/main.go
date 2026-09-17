// Command step_basic implements conformance requirement 1-1: a single step
// that takes input and returns a greeting string, succeeding on the first
// attempt.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
		return "Hello, " + name + "!", nil
	})
}

func main() {
	durable.Start(handler)
}
