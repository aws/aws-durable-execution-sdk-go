// Command child-context-basic demonstrates [durable.RunInChildContext]:
// grouping durable operations into a named sub-workflow whose result is
// checkpointed as a single unit.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.RunInChildContext(ctx, "child", func(child durable.Context) (string, error) {
		return durable.Step(child, "inner-step", func(_ durable.StepContext) (string, error) {
			return "child step completed", nil
		})
	})
}

func main() { durable.Start(handler) }
