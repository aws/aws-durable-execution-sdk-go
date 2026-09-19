// Command wait_for_callback_in_child_context implements conformance requirement 7-8:
// wait-for-callback nested inside a child context named "wrapper".
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	return durable.RunInChildContext(ctx, "wrapper", func(childCtx durable.Context) (string, error) {
		return durable.WaitForCallback[string](childCtx, name,
			func(_ durable.StepContext, _ string) error { return nil })
	})
}

func main() { durable.Start(handler) }
