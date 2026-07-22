// Command no-replay-execution demonstrates step execution tracking when
// no replay occurs: two sequential steps execute on the first invocation
// and the handler completes without needing a second invocation.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

// Result captures completion status.
type Result struct {
	Completed bool `json:"completed"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	_, err := durable.Step(ctx, "fetch-user-1", func(_ durable.StepContext) (string, error) {
		return "user-1", nil
	})
	if err != nil {
		return Result{}, err
	}

	_, err = durable.Step(ctx, "fetch-user-2", func(_ durable.StepContext) (string, error) {
		return "user-2", nil
	})
	if err != nil {
		return Result{}, err
	}

	return Result{Completed: true}, nil
}

func main() { durable.Start(handler) }
