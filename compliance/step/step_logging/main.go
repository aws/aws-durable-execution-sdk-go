// Command step_logging implements conformance requirement 1-7: a step
// that emits log entries through the step context logger.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	return durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("Greeting step started for: " + name)
		result := "Hello, " + name + "!"
		sc.Logger().Info("Greeting step completed with: " + result)
		return result, nil
	})
}

func main() {
	durable.Start(handler)
}
