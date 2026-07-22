// Command logger-log-levels demonstrates all log levels available on the
// durable context logger: Debug, Info, Warn, and Error. It also shows
// logging from within a step context and across a replay boundary (after a
// wait). During replay, all previously-emitted log lines are suppressed,
// proving that the default logger is replay-aware.
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ any) (string, error) {
	ctx.Logger().Info("=== Logger Level Demo Starting ===")

	ctx.Logger().Debug("debug-message", "detail", "verbose tracing information")
	ctx.Logger().Info("info-message", "detail", "general execution information")
	ctx.Logger().Warn("warn-message", "detail", "something might need attention")
	ctx.Logger().Error("error-message", "detail", "simulated error condition")

	// Log from within a step body via StepContext.Logger().
	_, err := durable.Step(ctx, "log-level-step", func(sc durable.StepContext) (string, error) {
		sc.Logger().Debug("step-debug", "source", "step-context")
		sc.Logger().Info("step-info", "source", "step-context")
		sc.Logger().Warn("step-warn", "source", "step-context")
		sc.Logger().Error("step-error", "source", "step-context")
		return "step completed", nil
	})
	if err != nil {
		return "", err
	}

	// Log with structured data (key-value pairs).
	ctx.Logger().Info("structured-data",
		"user", "TestUser",
		"userId", 12345,
		"action", "completed",
	)

	// Force a replay boundary — on the second invocation the log lines
	// above are suppressed, and only lines after this wait are live.
	ctx.Logger().Info("before-wait")
	if err := durable.Wait(ctx, "pause", 1*time.Second); err != nil {
		return "", err
	}
	ctx.Logger().Info("after-wait", "phase", "resumed")

	// Log from a child context.
	_, err = durable.RunInChildContext(ctx, "child-context", func(childCtx durable.Context) (string, error) {
		childCtx.Logger().Info("child-info", "source", "child-context")
		childCtx.Logger().Warn("child-warn", "source", "child-context")
		return "child completed", nil
	})
	if err != nil {
		return "", err
	}

	// Log an error with fmt.Errorf-style detail.
	testErr := fmt.Errorf("structured error test: code=%d", 42)
	ctx.Logger().Error("error-object", "err", testErr.Error())

	ctx.Logger().Info("=== Logger Level Demo Complete ===")

	return "done", nil
}

func main() { durable.Start(handler) }
