// Command logger-slog-handler demonstrates installing an application's own
// [slog.Handler] with [durable.WithLogHandler]. The handler here writes
// JSON to stdout with snake_case field names and a fixed service field, the
// shape a Lambda logging library such as Powertools produces. The SDK adds
// the execution and operation identifiers to that handler as structured
// attributes, and it suppresses records while a context replays, exactly as
// it does for the default handler.
package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// serviceName is attached to every record, as a service logger would.
const serviceName = "logger-slog-handler"

// snakeCaseKeys maps the SDK's attribute names to the snake_case names
// this application's log queries expect.
var snakeCaseKeys = map[string]string{
	"requestId":     "request_id",
	"executionArn":  "execution_arn",
	"operationId":   "operation_id",
	"operationName": "operation_name",
}

// newHandler builds the application's handler: JSON lines to w at DEBUG
// level, slog's time and msg keys renamed, the SDK's identifiers renamed to
// snake_case, and a service field on every record.
func newHandler(w io.Writer) slog.Handler {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				return slog.String("timestamp", a.Value.Time().UTC().Format(time.RFC3339Nano))
			case slog.MessageKey:
				a.Key = "message"
			}
			if renamed, ok := snakeCaseKeys[a.Key]; ok {
				a.Key = renamed
			}
			return a
		},
	})
	return h.WithAttrs([]slog.Attr{slog.String("service", serviceName)})
}

func handler(ctx durable.Context, _ any) (string, error) {
	ctx.Logger().Info("=== Logger Level Demo Starting ===")

	// Every level reaches the handler; the handler's own level decides
	// what is written.
	ctx.Logger().Debug("Debug message: Detailed debugging information")
	ctx.Logger().Info("Info message: General information about execution")
	ctx.Logger().Warn("Warning message: Something might need attention")
	ctx.Logger().Error("Error message: Something went wrong (simulated)")

	// Records emitted before the wait are suppressed when the second
	// invocation replays them. The context leaves replay when it claims
	// the first operation that has no checkpoint, so the line after the
	// wait is logged once the child context below has been claimed.
	ctx.Logger().Info("Before wait operation")
	if err := durable.Wait(ctx, "pause", 1*time.Second); err != nil {
		return "", err
	}

	// A child context's logger carries the same execution attributes plus
	// the child operation's operation_id and operation_name.
	_, err := durable.RunInChildContext(ctx, "child-context", func(childCtx durable.Context) (string, error) {
		childCtx.Logger().Info("Info log from child context")
		childCtx.Logger().Debug("Debug log from child context")
		childCtx.Logger().Warn("Warning log from child context")
		childCtx.Logger().Error("Error from child context with error value",
			"err", errors.New("child context error"))
		return "child context completed", nil
	})
	if err != nil {
		return "", err
	}
	ctx.Logger().Info("After wait operation - logger still works")

	// A step's logger adds operation_id, operation_name, and attempt.
	_, err = durable.Step(ctx, "direct-object-step", func(sc durable.StepContext) (string, error) {
		sc.Logger().Info("message", "stepData", "value", "num", 42)
		sc.Logger().Error("message", "err", errors.New("step context direct error"))
		return "direct object step completed", nil
	})
	if err != nil {
		return "", err
	}

	ctx.Logger().Error("Errors in context", "err", errors.New("first error"))

	return "done", nil
}

func main() {
	durable.Start(handler, durable.WithLogHandler(newHandler(os.Stdout)))
}
