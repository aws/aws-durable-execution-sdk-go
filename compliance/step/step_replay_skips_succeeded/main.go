// Command step_replay_skips_succeeded implements conformance requirement
// 1-9: on replay, a succeeded step returns its checkpointed result without
// re-executing (the log line proves single execution).
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	result, err := durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("step executed")
		return "cached_value", nil
	})
	if err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "", time.Second); err != nil {
		return "", err
	}
	return result, nil
}

func main() {
	durable.Start(handler)
}
