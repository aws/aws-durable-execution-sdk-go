// Command parallel-wait demonstrates parallel branches that each perform
// a wait of different durations, showing concurrent suspension.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	_, err := durable.Parallel(ctx, "parent-block", []durable.Branch[any]{
		{Name: "wait-1s", Func: func(ctx durable.Context) (any, error) {
			_ = durable.Wait(ctx, "wait-1-second", 1*time.Second)
			return nil, nil
		}},
		{Name: "wait-1s-again", Func: func(ctx durable.Context) (any, error) {
			_ = durable.Wait(ctx, "wait-1-second-again", 1*time.Second)
			return nil, nil
		}},
		{Name: "wait-2s", Func: func(ctx durable.Context) (any, error) {
			_ = durable.Wait(ctx, "wait-2-seconds", 2*time.Second)
			return nil, nil
		}},
		{Name: "wait-5s", Func: func(ctx durable.Context) (any, error) {
			_ = durable.Wait(ctx, "wait-5-seconds", 5*time.Second)
			return nil, nil
		}},
	})
	if err != nil {
		return "", err
	}

	return "Completed waits", nil
}

func main() { durable.Start(handler) }
