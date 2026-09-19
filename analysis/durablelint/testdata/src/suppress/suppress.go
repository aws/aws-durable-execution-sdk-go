package suppress

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func work(durable.StepContext) (string, error) { return "", nil }

func lineDirectives(ctx durable.Context) {
	go func() {
		durable.Step(ctx, "trailing", work) //durable:ignore
	}()

	go func() {
		//durable:ignore
		durable.Step(ctx, "above", work)
	}()

	go func() {
		//durable:ignore durablegoroutine -- the goroutine is joined before the next checkpoint
		durable.Step(ctx, "named", work)
	}()

	go func() {
		//durable:ignore durablenestedop, durablechildctx
		durable.Step(ctx, "other-rule", work) // want `durable.Step runs on a goroutine started by the go statement`
	}()

	go func() {
		//durable:ignored
		durable.Step(ctx, "not-a-directive", work) // want `durable.Step runs on a goroutine started by the go statement`
	}()

	go func() {
		//durable:ignore

		durable.Step(ctx, "blank-line-between", work) // want `durable.Step runs on a goroutine started by the go statement`
	}()

	go func() {
		durable.Wait(ctx, "multi-line", //durable:ignore
			time.Second)
	}()

	// A trailing directive covers only its own line.
	go func() {
		durable.Step(ctx, "trailing-first", work) //durable:ignore
		durable.Step(ctx, "next-line", work) // want `durable.Step runs on a goroutine started by the go statement`
	}()

	// Overlapping directives: the standalone one covers the next line, the
	// trailing one on that line adds to it, and neither reaches the line
	// after.
	go func() {
		//durable:ignore durablenestedop
		durable.Step(ctx, "both", work) //durable:ignore durablegoroutine
		durable.Step(ctx, "after-both", work) // want `durable.Step runs on a goroutine started by the go statement`
	}()

	// An every-rule directive is not narrowed by a later rule list on the
	// same line.
	go func() {
		//durable:ignore
		durable.Step(ctx, "all-then-named", work) //durable:ignore durablenestedop
	}()

	// Adjacent standalone directives each cover their own next line.
	go func() {
		//durable:ignore durablenestedop
		//durable:ignore durablegoroutine
		durable.Step(ctx, "second-standalone", work)
		durable.Step(ctx, "after-adjacent", work) // want `durable.Step runs on a goroutine started by the go statement`
	}()
}
