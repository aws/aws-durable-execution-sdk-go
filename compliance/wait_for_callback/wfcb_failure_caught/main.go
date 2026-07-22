// Command wfcb_failure_caught implements conformance requirement 7-6:
// wait-for-callback where failure is caught and "recovered" returned.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	result, err := durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil })
	if err != nil {
		return "recovered", nil
	}
	return result, nil
}

func main() { durable.Start(handler) }
