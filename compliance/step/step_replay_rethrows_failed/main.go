// Command step_replay_rethrows_failed implements conformance requirement
// 1-10: on replay, a permanently failed step re-returns the recorded error
// without re-executing (the log line proves single execution).
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	_, err := durable.Step(ctx, "", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("step executed")
		return "", errors.New("Something went wrong")
	}, durable.WithRetry(durable.NoRetry()))
	var stepErr *durable.StepError
	if !errors.As(err, &stepErr) {
		return "", err
	}

	if err := durable.Wait(ctx, "", time.Second); err != nil {
		return "", err
	}
	return "caught: " + stepErr.Error(), nil
}

func main() {
	durable.Start(handler)
}
