// Command future-race-wait demonstrates durable.Race with WaitAsync futures:
// race multiple wait timers and return as soon as the shortest completes.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result reports elapsed time demonstrating the race resolved at ~1s, not 10s.
type Result struct {
	ElapsedMs int64 `json:"elapsedMs"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	before, err := durable.Step(ctx, "before", func(_ durable.StepContext) (int64, error) {
		return time.Now().UnixMilli(), nil
	})
	if err != nil {
		return Result{}, err
	}

	w1 := durable.WaitAsync(ctx, "wait-1s", 1*time.Second)
	w2 := durable.WaitAsync(ctx, "wait-10s", 10*time.Second)

	_, err = durable.Race(ctx, "race-waits", []*durable.Future[durable.Void]{w1, w2})
	if err != nil {
		return Result{}, err
	}

	after, err := durable.Step(ctx, "after", func(_ durable.StepContext) (int64, error) {
		return time.Now().UnixMilli(), nil
	})
	if err != nil {
		return Result{}, err
	}

	return Result{ElapsedMs: after - before}, nil
}

func main() { durable.Start(handler) }
