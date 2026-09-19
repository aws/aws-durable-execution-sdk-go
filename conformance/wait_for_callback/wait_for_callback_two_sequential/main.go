// Command wait_for_callback_two_sequential implements conformance requirement 7-9:
// two sequential wait-for-callback operations.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	_, err := durable.WaitForCallback[string](ctx, "first",
		func(_ durable.StepContext, _ string) error { return nil })
	if err != nil {
		return "", err
	}
	return durable.WaitForCallback[string](ctx, "second",
		func(_ durable.StepContext, _ string) error { return nil })
}

func main() { durable.Start(handler) }
