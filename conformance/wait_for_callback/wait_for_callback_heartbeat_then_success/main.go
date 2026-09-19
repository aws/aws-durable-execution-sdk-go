// Command wait_for_callback_heartbeat_then_success implements conformance requirement 7-13:
// wait-for-callback with 10s heartbeat timeout, heartbeat sent then success.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	return durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil },
		durable.WithCallbackHeartbeatTimeout(10*time.Second))
}

func main() { durable.Start(handler) }
