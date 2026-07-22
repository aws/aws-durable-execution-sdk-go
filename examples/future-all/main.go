// Command future-all demonstrates durable.All: wait for all futures to
// succeed, returning results in input order. If any future fails, All fails
// immediately with the first error (fail-fast semantics).
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	f1 := durable.StepAsync(ctx, "step-1", func(_ durable.StepContext) (string, error) {
		return "result 1", nil
	})
	f2 := durable.StepAsync(ctx, "step-2", func(_ durable.StepContext) (string, error) {
		return "result 2", nil
	})
	f3 := durable.StepAsync(ctx, "step-3", func(_ durable.StepContext) (string, error) {
		return "result 3", nil
	})

	results, err := durable.All(ctx, "all-steps", []*durable.Future[string]{f1, f2, f3})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func main() { durable.Start(handler) }
