// Command child-context-virtual demonstrates [durable.RunInChildContext]
// with [durable.WithChildVirtual]. A virtual child context groups the
// operations inside it like any child context, but it is not an operation
// itself: the execution history records the step alone, with no
// ContextStarted or ContextSucceeded event for the wrapper, and the step is
// a top-level operation of the execution. The child body runs again on
// every invocation that reaches it; the step inside replays from its own
// checkpoint.
//
// For the concurrent child context pattern see concurrent-operations and
// future-join, which use [durable.Go].
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.RunInChildContext(ctx, "virtual-child", func(child durable.Context) (string, error) {
		return durable.Step(child, "inner-step", func(_ durable.StepContext) (string, error) {
			return "virtual child step completed", nil
		})
	}, durable.WithChildVirtual())
}

func main() { durable.Start(handler) }
