// Command callback_with_name implements conformance requirement 4-2:
// create callback with explicit name "approval".
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, "approval")
	if err != nil {
		return "", err
	}
	return cb.Result()
}

func main() { durable.Start(handler) }
