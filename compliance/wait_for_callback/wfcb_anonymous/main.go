// Command wfcb_anonymous implements conformance requirement 7-3:
// wait-for-callback with no operation name.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.WaitForCallback[string](ctx, "",
		func(_ durable.StepContext, _ string) error { return nil })
}

func main() { durable.Start(handler) }
