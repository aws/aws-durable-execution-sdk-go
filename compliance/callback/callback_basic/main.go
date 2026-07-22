// Command callback_basic implements conformance requirement 4-1: create a
// callback using the input as the name, block on result, return it.
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
