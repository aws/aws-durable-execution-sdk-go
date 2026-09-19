// Command wait_for_callback_basic implements conformance requirement 7-1: basic
// wait-for-callback with a no-op submitter.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	return durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil })
}

func main() { durable.Start(handler) }
