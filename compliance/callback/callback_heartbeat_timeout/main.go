// Command callback_heartbeat_timeout implements conformance requirement
// 4-4: create callback with 5-second heartbeat timeout, no heartbeat sent.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	cb, err := durable.CreateCallback[string](ctx, name,
		durable.WithCallbackHeartbeatTimeout(5*time.Second))
	if err != nil {
		return "", err
	}
	return cb.Result()
}

func main() { durable.Start(handler) }
