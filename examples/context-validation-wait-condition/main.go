// Command context-validation-wait-condition demonstrates the goroutine
// ownership error when user code calls a durable operation on the parent
// context from within a durable.Go child goroutine — specifically inside
// a WaitForCondition check function.
//
// The goroutine ownership diagnostic requires the "durablecheck" build tag.
// Without it, the check is a no-op and this handler succeeds instead of
// failing.
//
// Go adaptation note: The JS SDK validates context scope at the call site.
// In Go, the enforcement is goroutine ownership: using a parent context
// from a child goroutine triggers [durable.ErrWrongGoroutine].
//
// Expected terminal state: FAILED.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	ctx.Logger().Info("starting context validation")

	// Launch concurrent work — Go gives the child its own context.
	f := durable.Go(ctx, "child-goroutine", func(childCtx durable.Context) (int, error) {
		// ❌ WRONG: Using parent ctx from child goroutine inside
		// WaitForCondition. This triggers ErrWrongGoroutine.
		return durable.WaitForCondition(childCtx, "check-loop",
			func(_ durable.StepContext, state int) (int, error) {
				// Using parent context here triggers the ownership error.
				_, err := durable.Step(ctx, "wrong-parent-step", func(_ durable.StepContext) (string, error) {
					return "should not execute", nil
				})
				if err != nil {
					return 0, err
				}
				return state + 1, nil
			},
			durable.ConditionConfig[int]{
				InitialState: 0,
				WaitStrategy: func(state int, attempt int) durable.WaitDecision {
					if attempt >= 3 {
						return durable.WaitDecision{Continue: false}
					}
					return durable.WaitDecision{Continue: true, Delay: 1 * time.Second}
				},
			},
		)
	})

	result, err := f.Result()
	if err != nil {
		return "", err
	}

	_ = result
	return "should not reach here", nil
}

func main() { durable.Start(handler) }
