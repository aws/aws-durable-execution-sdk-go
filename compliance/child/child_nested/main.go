// Command child_nested implements conformance requirement 3-6: nested
// child contexts, each with a step. The outer child has a step then an
// inner child with its own step.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, event string) (string, error) {
	return durable.RunInChildContext(ctx, "outer", func(outer durable.Context) (string, error) {
		_, err := durable.Step(outer, "", func(_ durable.StepContext) (string, error) {
			return event, nil
		})
		if err != nil {
			return "", err
		}

		return durable.RunInChildContext(outer, "inner", func(inner durable.Context) (string, error) {
			return durable.Step(inner, "", func(_ durable.StepContext) (string, error) {
				return event, nil
			})
		})
	})
}

func main() {
	durable.Start(handler)
}
