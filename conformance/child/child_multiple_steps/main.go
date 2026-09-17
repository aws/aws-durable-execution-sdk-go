// Command child_multiple_steps implements conformance requirement 3-3: a
// child context with two sequential steps where the second receives the
// first step's result.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, event string) (string, error) {
	return durable.RunInChildContext(ctx, "multi-step", func(child durable.Context) (string, error) {
		result1, err := durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return event, nil
		})
		if err != nil {
			return "", err
		}
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return result1, nil
		})
	})
}

func main() {
	durable.Start(handler)
}
