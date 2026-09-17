// Command wfcb_json_result implements conformance requirement 7-11:
// wait-for-callback with structured JSON result deserialization.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

type approvalResult struct {
	Status string `json:"status"`
}

func handler(ctx durable.Context, name string) (string, error) {
	result, err := durable.WaitForCallback[approvalResult](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil })
	if err != nil {
		return "", err
	}
	return result.Status, nil
}

func main() { durable.Start(handler) }
