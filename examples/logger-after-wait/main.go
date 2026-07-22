// Command logger-after-wait demonstrates replay-aware logging across a
// suspend/resume boundary. Log lines emitted before the wait appear once
// (during the first invocation). On replay (after the wait completes), the
// replayed code path is suppressed — only log lines emitted during live
// execution (after the replay→live transition) appear in the second
// invocation.
//
// The replay→live transition happens when the next durable operation is
// claimed and no checkpoint exists for it. So a log call between the wait
// and the next operation is still suppressed. The log after a live step
// demonstrates the transition clearly.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	ctx.Logger().Info("before-wait", "phase", "live")

	if err := durable.Wait(ctx, "pause", 2*time.Second); err != nil {
		return "", err
	}

	// This step triggers the replay→live transition on the second
	// invocation. The step itself is live (not checkpointed), so the
	// logger switches from replay to live mode when claiming it.
	_, err := durable.Step(ctx, "post-wait-step", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("inside-post-wait-step", "phase", "live")
		return "ok", nil
	})
	if err != nil {
		return "", err
	}

	ctx.Logger().Info("after-wait-and-step", "phase", "resumed")

	return "done", nil
}

func main() { durable.Start(handler) }
