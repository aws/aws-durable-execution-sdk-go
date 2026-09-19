// Command wait_for_callback_external_failure implements conformance requirement 7-4:
// wait-for-callback where external system reports failure, uncaught.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	return durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil })
}

func main() { durable.Start(handler) }
