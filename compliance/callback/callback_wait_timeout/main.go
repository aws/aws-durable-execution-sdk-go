// Command callback_wait_timeout implements conformance requirement 4-11:
// create callback with 3s timeout, wait 6s, then await (timeout during wait).
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name,
		durable.WithCallbackTimeout(3*time.Second))
	if err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "delay", 6*time.Second); err != nil {
		return "", err
	}
	return cb.Result()
}

func main() { durable.Start(handler) }
