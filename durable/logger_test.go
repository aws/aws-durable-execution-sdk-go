package durable

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDefaultLoggerSuppressesDuringReplay(t *testing.T) {
	// The defaultLogger uses os.Stderr which we can't easily capture,
	// but we can verify the replay suppression logic by testing
	// setReplaying + the guard.
	logger := newDefaultLogger("arn:test")
	logger.setReplaying(true)

	// During replay, nothing should be emitted. Since we can't capture
	// os.Stderr in a unit test without redirection, verify via the
	// atomic flag that the guard is active.
	if !logger.replaying.Load() {
		t.Fatal("expected replaying = true")
	}

	logger.setReplaying(false)
	if logger.replaying.Load() {
		t.Fatal("expected replaying = false")
	}
}

func TestReplayAwareLoggerSuppressesDuringReplay(t *testing.T) {
	var buf bytes.Buffer
	inner := NewWriterLogger(&buf)
	replaying := &atomic.Bool{}
	logger := &replayAwareLogger{inner: inner, replaying: replaying}

	// Not replaying — should emit.
	logger.Info("visible")
	if !strings.Contains(buf.String(), "visible") {
		t.Fatalf("expected 'visible' in output, got: %s", buf.String())
	}
	buf.Reset()

	// Replaying — should suppress.
	replaying.Store(true)
	logger.Info("hidden")
	logger.Debug("hidden")
	logger.Warn("hidden")
	logger.Error("hidden")
	if buf.Len() != 0 {
		t.Fatalf("expected no output during replay, got: %s", buf.String())
	}

	// After replay ends — should emit again.
	replaying.Store(false)
	logger.Warn("back")
	if !strings.Contains(buf.String(), "back") {
		t.Fatalf("expected 'back' in output, got: %s", buf.String())
	}
}

func TestNopLoggerDiscards(t *testing.T) {
	// NopLogger must not panic.
	var l NopLogger
	l.Debug("msg", "key", "val")
	l.Info("msg")
	l.Warn("msg")
	l.Error("msg")
}

func TestWriterLoggerOutput(t *testing.T) {
	var buf bytes.Buffer
	l := NewWriterLogger(&buf)
	l.Info("hello", "count", 42)

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("failed to parse log output: %v", err)
	}
	if record["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", record["level"])
	}
	if record["message"] != "hello" {
		t.Errorf("message = %v, want hello", record["message"])
	}
	if record["count"] != float64(42) {
		t.Errorf("count = %v, want 42", record["count"])
	}
	if _, ok := record["timestamp"]; !ok {
		t.Error("missing timestamp")
	}
}

func TestWriterLoggerAllLevels(t *testing.T) {
	levels := []struct {
		name string
		fn   func(*WriterLogger, string, ...any)
		want string
	}{
		{"Debug", (*WriterLogger).Debug, "DEBUG"},
		{"Info", (*WriterLogger).Info, "INFO"},
		{"Warn", (*WriterLogger).Warn, "WARN"},
		{"Error", (*WriterLogger).Error, "ERROR"},
	}
	for _, tt := range levels {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := NewWriterLogger(&buf)
			tt.fn(l, "test")
			var record map[string]any
			if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			if record["level"] != tt.want {
				t.Errorf("level = %v, want %s", record["level"], tt.want)
			}
		})
	}
}

func TestReplayTogglerInterface(t *testing.T) {
	// Verify both logger types implement the toggle interface.
	var _ replayToggler = newDefaultLogger("arn")
	var _ replayToggler = &replayAwareLogger{inner: NopLogger{}, replaying: &atomic.Bool{}}
}

func TestExecContextReplayToggle(t *testing.T) {
	// Create an exec context in replay mode and verify logger is toggled.
	var buf bytes.Buffer
	inner := NewWriterLogger(&buf)
	replaying := &atomic.Bool{}
	logger := &replayAwareLogger{inner: inner, replaying: replaying}

	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusSucceeded},
	})

	ec := newExecContext(t.Context(), "arn:test", invocationInfo{}, logger, state)
	// Should be in replay mode.
	if !ec.IsReplaying() {
		t.Fatal("expected replay mode")
	}
	if !replaying.Load() {
		t.Fatal("expected logger replaying = true")
	}

	// After claiming all replayed ops and hitting execution mode:
	ec.owner = currentGoroutineOwner()
	_, _ = ec.claimOperation() // claims op "1" (replayed)
	// Next claim flips to execution.
	ec.refreshReplayMode()
	if replaying.Load() {
		t.Fatal("expected logger replaying = false after flip to execution")
	}
}

func TestUserProvidedLoggerWrappedForReplaySuppression(t *testing.T) {
	// A user-provided logger (WriterLogger) does not implement
	// replayToggler directly. When wrapped with replayAwareLogger, it
	// should suppress output during replay — the same wrapping that
	// handler.go applies for WithLogger users.
	var buf bytes.Buffer
	userLogger := NewWriterLogger(&buf)

	// Wrap as the handler does.
	wrapped := &replayAwareLogger{inner: userLogger, replaying: &atomic.Bool{}}

	// Simulate replay state: pass wrapped logger into newExecContext with
	// replayed operations.
	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusSucceeded},
	})
	ec := newExecContext(t.Context(), "arn:test:user-logger", invocationInfo{}, wrapped, state)

	// Context is in replay mode.
	if !ec.IsReplaying() {
		t.Fatal("expected replay mode")
	}

	// Log via context's logger — should be suppressed.
	ec.Logger().Info("should-be-hidden")
	if buf.Len() != 0 {
		t.Fatalf("expected no output during replay, got: %s", buf.String())
	}

	// Transition out of replay.
	ec.owner = currentGoroutineOwner()
	_, _ = ec.claimOperation()
	ec.refreshReplayMode()

	// Log again — should now emit.
	ec.Logger().Info("should-be-visible")
	if !strings.Contains(buf.String(), "should-be-visible") {
		t.Fatalf("expected 'should-be-visible' in output, got: %s", buf.String())
	}
}
