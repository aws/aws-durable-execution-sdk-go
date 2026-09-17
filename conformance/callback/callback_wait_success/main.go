// Command callback_wait_success implements conformance requirement 4-9:
// create callback, wait 5s, then await callback result (success).
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name)
	if err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "delay", 5*time.Second); err != nil {
		return "", err
	}
	return cb.Result()
}

func main() { durable.Start(handler) }
