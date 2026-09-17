// Command child_with_name implements conformance requirement 3-2: a named
// child context containing a single step that returns the value field.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

type input struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func handler(ctx durable.Context, event input) (string, error) {
	return durable.RunInChildContext(ctx, event.Name, func(child durable.Context) (string, error) {
		return durable.Step(child, "", func(_ durable.StepContext) (string, error) {
			return event.Value, nil
		})
	})
}

func main() {
	durable.Start(handler)
}
