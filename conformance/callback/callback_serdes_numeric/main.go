// Command callback_serdes_numeric implements conformance requirement 4-16:
// custom deserializer converts callback payload into a number.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

type numericResult struct {
	Count   int `json:"count"`
	Doubled int `json:"doubled"`
}

func handler(ctx durable.Context, name string) (numericResult, error) {
	cb, err := durable.CreateCallback[int](ctx, name)
	if err != nil {
		return numericResult{}, err
	}
	value, err := cb.Result()
	if err != nil {
		return numericResult{}, err
	}
	return numericResult{Count: value, Doubled: value * 2}, nil
}

func main() { durable.Start(handler) }
