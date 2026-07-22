// Command wait-configurable demonstrates [durable.Wait] with a
// configurable duration derived from the event payload.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input accepts an optional WaitSeconds field from the event payload.
type Input struct {
	WaitSeconds int `json:"waitSeconds"`
}

func handler(ctx durable.Context, event Input) (string, error) {
	seconds := event.WaitSeconds
	if seconds <= 0 {
		seconds = 2
	}
	if err := durable.Wait(ctx, "configurable-wait", time.Duration(seconds)*time.Second); err != nil {
		return "", err
	}
	return "wait finished", nil
}

func main() { durable.Start(handler) }
