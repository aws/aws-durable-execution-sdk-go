// Command context-validation-step demonstrates the goroutine ownership error
// when user code calls a durable operation on the parent context from within
// a durable.Go child goroutine — specifically inside a step-like pattern.
//
// The goroutine ownership check runs in every default build. Building with
// -tags durablenocheck compiles it out, and this handler then succeeds
// instead of failing.
//
// In Go, context scope is enforced through goroutine ownership: using a
// parent context from a child goroutine triggers [durable.ErrWrongGoroutine].
//
// Expected terminal state: FAILED.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	ctx.Logger().Info("starting context validation")

	// Launch concurrent work — Go gives the child its own context.
	f := durable.Go(ctx, "child-goroutine", func(childCtx durable.Context) (string, error) {
		// Correct: use childCtx for child operations.
		_, err := durable.Step(childCtx, "correct-step", func(_ durable.StepContext) (string, error) {
			return "this works", nil
		})
		if err != nil {
			return "", err
		}

		// ❌ WRONG: Using parent ctx from child goroutine inside step.
		// This triggers ErrWrongGoroutine because ctx is owned by
		// the parent goroutine, not this goroutine.
		//durable:ignore -- this example demonstrates the violation on purpose
		_, err = durable.Step(ctx, "wrong-parent-step", func(_ durable.StepContext) (string, error) {
			return "should not execute", nil
		})
		if err != nil {
			return "", err
		}

		return "should not reach here", nil
	})

	result, err := f.Result()
	if err != nil {
		return "", err
	}

	return result, nil
}

func main() { durable.Start(handler) }
