// Command logger-after-wait demonstrates replay-aware logging across a
// suspend/resume boundary. The line before the wait is written once,
// during the first invocation. The second invocation replays the code
// before the wait and drops its records. The wait's outcome is new to the
// second invocation, so the code after the wait is live there and its
// lines are written.
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

	// The step runs live on the second invocation, after the wait.
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
