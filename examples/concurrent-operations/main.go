// Command concurrent-operations demonstrates durable.Go for concurrent
// child-context operations joined with durable.All. Each child context
// runs its own steps and waits independently.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	task1 := durable.Go(ctx, "block-1", func(childCtx durable.Context) (string, error) {
		result, err := durable.Step(childCtx, "step-1", func(_ durable.StepContext) (string, error) {
			return "task 1 result", nil
		})
		if err != nil {
			return "", err
		}
		_ = durable.Wait(childCtx, "wait-1", 1*time.Second)
		return result, nil
	})

	task2 := durable.Go(ctx, "block-2", func(childCtx durable.Context) (string, error) {
		result, err := durable.Step(childCtx, "step-2", func(_ durable.StepContext) (string, error) {
			return "task 2 result", nil
		})
		if err != nil {
			return "", err
		}
		_ = durable.Wait(childCtx, "wait-2", 2*time.Second)
		return result, nil
	})

	results, err := durable.All(ctx, "join", []*durable.Future[string]{task1, task2})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func main() { durable.Start(handler) }
