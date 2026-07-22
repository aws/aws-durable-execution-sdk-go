// Command invoke-tenant-target is the target function for
// invoke-tenant-id. It performs a short wait then returns, simulating a
// tenant-isolated service.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input accepts an optional wait duration.
type Input struct {
	Seconds int `json:"seconds"`
}

func handler(ctx durable.Context, event Input) (string, error) {
	waitTime := time.Duration(event.Seconds) * time.Second
	if waitTime == 0 {
		waitTime = 1 * time.Second
	}
	if err := durable.Wait(ctx, "tenant-wait", waitTime); err != nil {
		return "", err
	}
	return "wait finished", nil
}

func main() { durable.Start(handler) }
