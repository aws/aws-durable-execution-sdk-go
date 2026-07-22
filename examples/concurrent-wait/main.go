// Command concurrent-wait demonstrates multiple WaitAsync futures running
// concurrently, joined with durable.All. The total wall-clock time is
// bounded by the longest wait, not the sum.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	w1 := durable.WaitAsync(ctx, "wait-1-second", 1*time.Second)
	w2 := durable.WaitAsync(ctx, "wait-3-seconds", 3*time.Second)
	w3 := durable.WaitAsync(ctx, "wait-5-seconds", 5*time.Second)

	_, err := durable.All(ctx, "all-waits", []*durable.Future[durable.Void]{w1, w2, w3})
	if err != nil {
		return "", err
	}

	return "Completed waits", nil
}

func main() { durable.Start(handler) }
