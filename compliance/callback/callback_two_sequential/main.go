// Command callback_two_sequential implements conformance requirement 4-17:
// create callback A, wait for A, create callback B, wait for B.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, names []string) (map[string]string, error) {
	cbA, err := durable.CreateCallback[string](ctx, names[0])
	if err != nil {
		return nil, err
	}
	resultA, err := cbA.Result()
	if err != nil {
		return nil, err
	}

	cbB, err := durable.CreateCallback[string](ctx, names[1])
	if err != nil {
		return nil, err
	}
	resultB, err := cbB.Result()
	if err != nil {
		return nil, err
	}

	return map[string]string{"a": resultA, "b": resultB}, nil
}

func main() { durable.Start(handler) }
