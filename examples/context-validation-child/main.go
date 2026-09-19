// Command context-validation-child demonstrates the error that occurs when
// user code calls a durable operation on a context from the wrong goroutine.
// The Go SDK enforces that each context is confined to its owning goroutine;
// calling a durable operation from another goroutine produces
// [durable.ErrWrongGoroutine].
//
// The goroutine ownership check runs in every default build. Building with
// -tags durablenocheck compiles it out, and this handler then succeeds
// instead of failing.
//
// In Go, context scope is enforced through goroutine ownership. Using
// durable.Go creates a new goroutine with its own context; attempting to
// use the parent context from that goroutine fails.
//
// Expected terminal state: FAILED (ErrWrongGoroutine propagates as handler error).
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

func handler(ctx durable.Context, _ any) (string, error) {
	ctx.Logger().Info("starting context validation")

	// Correct: using the context from the Go branch.
	f1 := durable.StepAsync(ctx, "correct-step", func(_ durable.StepContext) (string, error) {
		return "this works", nil
	})
	_, err := f1.Result()
	if err != nil {
		return "", err
	}

	// ❌ WRONG: Using the parent context from a Go-spawned goroutine.
	// durable.Go gives the function its own context, but we ignore it
	// and use the parent ctx instead. This triggers ErrWrongGoroutine.
	f2 := durable.Go(ctx, "child-goroutine", func(_ durable.Context) (string, error) {
		// This uses the PARENT ctx from a different goroutine — fails.
		//durable:ignore -- this example demonstrates the violation on purpose
		return durable.Step(ctx, "wrong-step", func(_ durable.StepContext) (string, error) {
			return "should not execute", nil
		})
	})

	result, err := f2.Result()
	if err != nil {
		return "", err
	}

	return result, nil
}

func main() { durable.Start(handler) }
