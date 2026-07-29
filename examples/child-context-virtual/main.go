// Command child-context-virtual demonstrates [durable.Go] — the
// concurrent child context pattern. Unlike RunInChildContext (synchronous),
// Go spawns the child on its own goroutine and returns a [*durable.Future]
// that the caller awaits.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	f := durable.Go(ctx, "virtual-child", func(child durable.Context) (string, error) {
		return durable.Step(child, "inner-step", func(_ durable.StepContext) (string, error) {
			return "virtual child step completed", nil
		})
	})
	return f.Result()
}

func main() { durable.Start(handler) }
