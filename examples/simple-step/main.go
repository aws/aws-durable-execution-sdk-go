// Command simple-step demonstrates the most basic use of [durable.Step]:
// a single checkpoint that records its result for replay.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	return durable.Step(ctx, "greet", func(_ durable.StepContext) (string, error) {
		return "step completed", nil
	})
}

func main() { durable.Start(handler) }
