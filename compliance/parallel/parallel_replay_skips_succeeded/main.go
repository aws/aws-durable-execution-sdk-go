// Command parallel_replay_skips_succeeded implements conformance requirement 8-14.
package main

import (
	"time"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) ([]string, error) {
	result, err := durable.Parallel(ctx, "replay", []durable.Branch[string]{
		{Func: func(childCtx durable.Context) (string, error) {
			return durable.Step(childCtx, "", func(_ durable.StepContext) (string, error) { return "b0", nil })
		}},
		{Func: func(childCtx durable.Context) (string, error) {
			if err := durable.Wait(childCtx, "", 2*time.Second); err != nil { return "", err }
			return "b1", nil
		}},
	}, durable.WithMaxConcurrency(1))
	if err != nil { return nil, err }
	return result.Results(), nil
}

func main() { durable.Start(handler) }
