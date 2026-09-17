// Command wait_multiple_sequential implements conformance requirement 2-3:
// two sequential wait operations, each suspending and resuming the
// execution.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type result struct {
	CompletedWaits int `json:"completedWaits"`
}

func handler(ctx durable.Context, _ any) (result, error) {
	if err := durable.Wait(ctx, "wait-1", 2*time.Second); err != nil {
		return result{}, err
	}
	if err := durable.Wait(ctx, "wait-2", 2*time.Second); err != nil {
		return result{}, err
	}
	return result{CompletedWaits: 2}, nil
}

func main() {
	durable.Start(handler)
}
