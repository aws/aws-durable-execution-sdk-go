// Command wfcb_explicit_name implements conformance requirement 7-2:
// wait-for-callback with explicit static name "approval".
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.WaitForCallback[string](ctx, "approval",
		func(_ durable.StepContext, _ string) error { return nil })
}

func main() { durable.Start(handler) }
