// Command child_basic implements conformance requirement 3-1: a child
// context containing a single step that returns the input string.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, event string) (string, error) {
	return durable.RunInChildContext(ctx, "", func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return event, nil
		})
	})
}

func main() {
	durable.Start(handler)
}
