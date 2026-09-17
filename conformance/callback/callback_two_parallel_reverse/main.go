// Command callback_two_parallel_reverse implements conformance requirement
// 4-19: create A, create B, wait for B, wait for A.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, names []string) (map[string]string, error) {
	cbA, err := durable.CreateCallback[string](ctx, names[0])
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
	resultA, err := cbA.Result()
	if err != nil {
		return nil, err
	}

	return map[string]string{"a": resultA, "b": resultB}, nil
}

func main() { durable.Start(handler) }
