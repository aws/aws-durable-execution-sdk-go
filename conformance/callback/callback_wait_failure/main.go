// Command callback_wait_failure implements conformance requirement 4-10:
// create callback, wait 5s, then await callback result (failure).
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
