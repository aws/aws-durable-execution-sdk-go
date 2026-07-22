// Command callback_heartbeat_success implements conformance requirement
// 4-5: create callback with 10-second heartbeat timeout; heartbeat resets
// the timer, then success is sent.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name,
		durable.WithCallbackHeartbeatTimeout(10*time.Second))
	if err != nil {
		return "", err
	}
	return cb.Result()
}

func main() { durable.Start(handler) }
