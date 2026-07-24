// Command future-unhandled-error demonstrates that failed futures used
// inside combinators do not cause unhandled errors during replay. Multiple
// timing scenarios verify that All (fail-fast) and Any (all-fail) both
// handle errors cleanly across wait boundaries.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Result reports the scenarios that completed without unhandled errors.
type Result struct {
	SuccessStep     string   `json:"successStep"`
	ScenariosTested []string `json:"scenariosTested"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	// Scenario 1: All with a failing future — catch the error.
	f1 := durable.StepAsync(ctx, "fail-1", func(_ durable.StepContext) (string, error) {
		return "", errors.New("this step failed")
	}, durable.WithRetry(durable.NoRetry()))

	_, err := durable.All(ctx, "all-1", []*durable.Future[string]{f1})
	if err != nil {
		// Error caught cleanly — no unhandled propagation.
		_ = err
	}

	_ = durable.Wait(ctx, "wait-after-basic", 1*time.Second)

	// Scenario 2: Multiple combinators, immediate usage.
	f2 := durable.StepAsync(ctx, "fail-2", func(_ durable.StepContext) (string, error) {
		return "", errors.New("step 1 failed")
	}, durable.WithRetry(durable.NoRetry()))
	f3 := durable.StepAsync(ctx, "fail-3", func(_ durable.StepContext) (string, error) {
		return "", errors.New("step 2 failed")
	}, durable.WithRetry(durable.NoRetry()))

	_, _ = durable.All(ctx, "all-2", []*durable.Future[string]{f2})
	_, _ = durable.Any(ctx, "any-1", []*durable.Future[string]{f3})

	// Scenario 3: Combinator after wait (replay path).
	f4 := durable.StepAsync(ctx, "fail-4", func(_ durable.StepContext) (string, error) {
		return "", errors.New("step 3 failed")
	}, durable.WithRetry(durable.NoRetry()))
	f5 := durable.StepAsync(ctx, "fail-5", func(_ durable.StepContext) (string, error) {
		return "", errors.New("step 4 failed")
	}, durable.WithRetry(durable.NoRetry()))

	_, _ = durable.All(ctx, "all-3", []*durable.Future[string]{f4})

	_ = durable.Wait(ctx, "wait-middle", 1*time.Second)

	_, _ = durable.Any(ctx, "any-2", []*durable.Future[string]{f5})

	// Scenario 4: Combinator after extended wait (deep replay).
	f6 := durable.StepAsync(ctx, "fail-6", func(_ durable.StepContext) (string, error) {
		return "", errors.New("step 5 failed")
	}, durable.WithRetry(durable.NoRetry()))
	f7 := durable.StepAsync(ctx, "fail-7", func(_ durable.StepContext) (string, error) {
		return "", errors.New("step 6 failed")
	}, durable.WithRetry(durable.NoRetry()))

	_ = durable.Wait(ctx, "wait-before-final", 1*time.Second)

	_, _ = durable.All(ctx, "all-4", []*durable.Future[string]{f6})
	_, _ = durable.Any(ctx, "any-3", []*durable.Future[string]{f7})

	// Final success step verifies execution completes after replay.
	val, err := durable.Step(ctx, "success-step", func(_ durable.StepContext) (string, error) {
		return "Success", nil
	})
	if err != nil {
		return Result{}, err
	}

	return Result{
		SuccessStep: val,
		ScenariosTested: []string{
			"basic-all-catch",
			"immediate-combinator-usage",
			"combinator-after-wait-replay",
			"combinator-after-extended-wait",
		},
	}, nil
}

func main() { durable.Start(handler) }
