// Command parallel-basic demonstrates a Parallel operation running
// multiple branches concurrently with bounded concurrency.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	results, err := durable.Parallel(ctx, "parallel", []durable.Branch[string]{
		{Name: "task-1", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
				return "task 1 completed", nil
			})
		}},
		{Name: "task-2", Func: func(ctx durable.Context) (string, error) {
			return durable.Step(ctx, "", func(_ durable.StepContext) (string, error) {
				return "task 2 completed", nil
			})
		}},
		{Name: "task-3", Func: func(ctx durable.Context) (string, error) {
			_ = durable.Wait(ctx, "branch-wait", 1*time.Second)
			return "task 3 completed after wait", nil
		}},
	}, durable.WithMaxConcurrency(2))
	if err != nil {
		return nil, err
	}

	return results.Results(), nil
}

func main() { durable.Start(handler) }
