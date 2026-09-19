// Command wait_for_callback_empty_result implements conformance requirement 7-15:
// wait-for-callback with empty/null payload.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (any, error) {
	return durable.WaitForCallback[any](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil })
}

func main() { durable.Start(handler) }
