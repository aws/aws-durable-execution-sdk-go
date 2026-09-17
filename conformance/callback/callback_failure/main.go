// Command callback_failure implements conformance requirement 4-6:
// create callback, external system reports failure, handler does not catch.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name)
	if err != nil {
		return "", err
	}
	return cb.Result()
}

func main() { durable.Start(handler) }
