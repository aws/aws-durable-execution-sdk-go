// Command wfcb_heartbeat_timeout implements conformance requirement 7-12:
// wait-for-callback with 5s heartbeat timeout, no heartbeat sent.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, name string) (string, error) {
	return durable.WaitForCallback[string](ctx, name,
		func(_ durable.StepContext, _ string) error { return nil },
		durable.WithCallbackHeartbeatTimeout(5*time.Second))
}

func main() { durable.Start(handler) }
