// Command future-all-wait demonstrates durable.All with WaitAsync futures:
// multiple wait timers run concurrently and All awaits all of them.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	w1 := durable.WaitAsync(ctx, "wait-1", 1*time.Second)
	w2 := durable.WaitAsync(ctx, "wait-2", 2*time.Second)
	f3 := durable.StepAsync(ctx, "step-3", func(_ durable.StepContext) (durable.Void, error) {
		return durable.Void{}, nil
	})

	_, err := durable.All(ctx, "all-waits", []*durable.Future[durable.Void]{w1, w2, f3})
	if err != nil {
		return "", err
	}

	return "all waits completed", nil
}

func main() { durable.Start(handler) }
