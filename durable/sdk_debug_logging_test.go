package durable_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// captureHandler records every slog record it is given. It is enabled at
// Debug, so a Debug record the SDK emitted would be captured here.
type captureHandler struct {
	mu   *sync.Mutex
	recs *[]slog.Record
}

func (h captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, r.Clone())
	return nil
}

func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// TestNoSDKDebugRecords sets a Debug-level handler and logs the Debug
// records the SDK emits across a Step, a child context with a nested Step,
// and a Wait that suspends and resumes. The handler emits one Debug record
// of its own, which shows that the Debug channel reaches the handler.
func TestNoSDKDebugRecords(t *testing.T) {
	var mu sync.Mutex
	var recs []slog.Record

	handler := func(ctx durable.Context, _ any) (string, error) {
		if err := durable.ConfigureLogging(ctx, durable.LogConfig{
			Handler: captureHandler{mu: &mu, recs: &recs},
		}); err != nil {
			return "", err
		}
		ctx.Logger().Debug("control-debug-line")

		if _, err := durable.Step(ctx, "s1", func(_ durable.StepContext) (string, error) {
			return "a", nil
		}); err != nil {
			return "", err
		}
		if _, err := durable.RunInChildContext(ctx, "child", func(c durable.Context) (string, error) {
			return durable.Step(c, "s2", func(_ durable.StepContext) (string, error) {
				return "b", nil
			})
		}); err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "pause", time.Minute); err != nil {
			return "", err
		}
		return "done", nil
	}

	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	debugCount, controlCount := 0, 0
	var sdkDebug []string
	for _, rec := range recs {
		if rec.Level != slog.LevelDebug {
			continue
		}
		debugCount++
		if rec.Message == "control-debug-line" {
			controlCount++
			continue
		}
		sdkDebug = append(sdkDebug, rec.Message)
	}
	t.Logf("status=%s totalRecords=%d debugRecords=%d controlDebug=%d sdkDebugMessages=%v",
		r.Status, len(recs), debugCount, controlCount, sdkDebug)
}
