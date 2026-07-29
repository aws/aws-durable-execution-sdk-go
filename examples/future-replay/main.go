// Command future-replay demonstrates that a failed future used inside
// AllSettled does not cause unhandled errors on replay. The workflow settles
// the failure gracefully, waits, then continues to a success step.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result reports the final success step output.
type Result struct {
	SuccessStep string `json:"successStep"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	failFuture := durable.StepAsync(ctx, "failure-step", func(_ durable.StepContext) (string, error) {
		return "", errors.New("this step failed")
	}, durable.WithRetry(durable.NoRetry()))

	// Wait for the async step to settle before consuming its future. A
	// step checkpoints its outcome before settling its future, so this
	// gate guarantees the failed step is recorded before the settle
	// context, keeping the operation log order deterministic. Done does
	// not consume the outcome: AllSettled still absorbs the failure.
	<-failFuture.Done()

	// AllSettled absorbs the failure — no unhandled error propagation.
	_, err := durable.AllSettled(ctx, "settle", []*durable.Future[string]{failFuture})
	if err != nil {
		return Result{}, err
	}

	// Wait introduces a replay boundary.
	_ = durable.Wait(ctx, "wait", 1*time.Second)

	// Continue with a success step after replay.
	val, err := durable.Step(ctx, "success-step", func(_ durable.StepContext) (string, error) {
		return "Success", nil
	})
	if err != nil {
		return Result{}, err
	}

	return Result{SuccessStep: val}, nil
}

func main() { durable.Start(handler) }
