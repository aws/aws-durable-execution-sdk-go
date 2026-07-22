// Command future-race demonstrates durable.Race: return the outcome of the
// first future to settle (succeed or fail). Race propagates the winner's
// error if the winner failed.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	f1 := durable.StepAsync(ctx, "slow", func(_ durable.StepContext) (string, error) {
		time.Sleep(2 * time.Second)
		return "slow result", nil
	})
	f2 := durable.StepAsync(ctx, "fast", func(_ durable.StepContext) (string, error) {
		return "fast result", nil
	})
	f3 := durable.StepAsync(ctx, "slower", func(_ durable.StepContext) (string, error) {
		time.Sleep(3 * time.Second)
		return "slower result", nil
	})

	result, err := durable.Race(ctx, "race", []*durable.Future[string]{f1, f2, f3})
	if err != nil {
		return "", err
	}

	return result, nil
}

func main() { durable.Start(handler) }
