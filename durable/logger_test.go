package durable

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// lockedBuffer is a goroutine-safe bytes.Buffer for capturing log output
// emitted from concurrent branches.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestReplayAwareLoggerSuppressesWhileReplaying(t *testing.T) {
	var buf bytes.Buffer
	replaying := false
	logger := newReplayAwareLogger(NewWriterLogger(&buf), func() bool { return replaying })

	// Not replaying: emit.
	logger.Info("visible")
	if !strings.Contains(buf.String(), "visible") {
		t.Fatalf("expected 'visible' in output, got: %s", buf.String())
	}
	buf.Reset()

	// Replaying: suppress every level.
	replaying = true
	logger.Info("hidden")
	logger.Debug("hidden")
	logger.Warn("hidden")
	logger.Error("hidden")
	if buf.Len() != 0 {
		t.Fatalf("expected no output during replay, got: %s", buf.String())
	}

	// Replay ended: emit again.
	replaying = false
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

func TestDefaultLoggerIsNotReplayAware(t *testing.T) {
	// The default logger carries no replay state of its own: suppression
	// is applied by the context that hands it out. It must still satisfy
	// Logger so it can be the base of a per-context wrapper.
	var _ Logger = newDefaultLogger("arn:test")
}

func TestExecContextLoggerReadsOwnReplayState(t *testing.T) {
	// The logger a context returns consults that context's own mode. No
	// shared flag is toggled: the context's replay state is the source of
	// truth on every call.
	var buf bytes.Buffer
	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusSucceeded},
	})
	ec := newExecContext(t.Context(), "arn:test", invocationInfo{}, NewWriterLogger(&buf), state)
	if !ec.IsReplaying() {
		t.Fatal("expected replay mode")
	}

	ec.Logger().Info("should-be-hidden")
	if buf.Len() != 0 {
		t.Fatalf("expected no output during replay, got: %s", buf.String())
	}

	// Claim the replayed operation, then refresh for the next (absent)
	// one: the context flips to live execution.
	ec.owner = currentGoroutineOwner()
	_, _ = ec.claimOperation()
	ec.refreshReplayMode()
	if ec.IsReplaying() {
		t.Fatal("expected execution mode after flip")
	}

	ec.Logger().Info("should-be-visible")
	if !strings.Contains(buf.String(), "should-be-visible") {
		t.Fatalf("expected 'should-be-visible' in output, got: %s", buf.String())
	}
}

func TestStepContextLoggerFollowsBranchReplayState(t *testing.T) {
	// A step context hands out the same per-branch logger as the context
	// that ran the step, so a step body logging while its branch replays
	// is suppressed and one logging while live is emitted.
	var buf bytes.Buffer
	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusSucceeded},
	})
	ec := newExecContext(t.Context(), "arn:test", invocationInfo{}, NewWriterLogger(&buf), state)

	sc := &stepContext{Context: ec.Context, logger: ec.Logger(), attempt: 1}
	sc.Logger().Info("hidden")
	if buf.Len() != 0 {
		t.Fatalf("expected no output during replay, got: %s", buf.String())
	}

	ec.mode.Store(int32(modeExecution))
	sc.Logger().Info("visible")
	if !strings.Contains(buf.String(), "visible") {
		t.Fatalf("expected 'visible' in output, got: %s", buf.String())
	}
}

func TestChildLoggerIsolatedFromSiblingReplayTransition(t *testing.T) {
	// Two child contexts of one replaying root. The first reaches live
	// execution; the second is still replaying. Only the first may log.
	var buf bytes.Buffer
	state := newExecutionState([]*operation{
		{id: hashID("exec"), status: statusStarted},
		{id: hashID("1"), status: statusStarted},
		{id: hashID("1-1"), status: statusSucceeded},
		{id: hashID("2"), status: statusStarted},
		{id: hashID("2-1"), status: statusSucceeded},
	})
	root := newExecContext(t.Context(), "arn:test", invocationInfo{}, NewWriterLogger(&buf), state)
	owner := currentGoroutineOwner()
	a := root.child("1", owner, modeReplay)
	b := root.child("2", owner, modeReplay)

	// a flips to live execution; b and the root stay in replay.
	a.mode.Store(int32(modeExecution))

	a.Logger().Info("a-live")
	b.Logger().Info("b-replaying")
	root.Logger().Info("root-replaying")

	out := buf.String()
	if !strings.Contains(out, "a-live") {
		t.Errorf("expected live branch line 'a-live' in output, got: %s", out)
	}
	if strings.Contains(out, "b-replaying") {
		t.Errorf("still-replaying sibling must stay suppressed, got: %s", out)
	}
	if strings.Contains(out, "root-replaying") {
		t.Errorf("still-replaying root must stay suppressed, got: %s", out)
	}
}

func TestGoBranchReplaySuppressionIsPerBranch(t *testing.T) {
	// Two Go branches replay from a checkpoint log. Branch "a" has one
	// checkpointed step and reaches live execution on its second step.
	// Branch "b" has two checkpointed steps and waits until "a" is live
	// before logging: at that point "b" is still replaying, so its line
	// must be suppressed. Once "b" runs its own live step, its line is
	// emitted.
	fake := &fakeLambda{}
	var out lockedBuffer
	payload := childPayload(`"x"`,
		checkpointedChild("1", "STARTED", nil),
		checkpointedStep("1-1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"a1"`}),
		checkpointedChild("2", "STARTED", nil),
		checkpointedStep("2-1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"b1"`}),
		checkpointedStep("2-2", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"b2"`}),
	)
	aLive := make(chan struct{})

	h := Wrap(func(ctx Context, _ string) (string, error) {
		a := Go(ctx, "a", func(c Context) (string, error) {
			if _, err := Step(c, "a1", func(StepContext) (string, error) { return "a1", nil }); err != nil {
				return "", err
			}
			c.Logger().Info("a-replaying")
			if _, err := Step(c, "a2", func(StepContext) (string, error) { return "a2", nil }); err != nil {
				return "", err
			}
			c.Logger().Info("a-live")
			close(aLive)
			return "a", nil
		})
		b := Go(ctx, "b", func(c Context) (string, error) {
			if _, err := Step(c, "b1", func(StepContext) (string, error) { return "b1", nil }); err != nil {
				return "", err
			}
			if _, err := Step(c, "b2", func(StepContext) (string, error) { return "b2", nil }); err != nil {
				return "", err
			}
			<-aLive
			if !c.IsReplaying() {
				t.Error("branch b must still be replaying after its checkpointed steps")
			}
			c.Logger().Info("b-still-replaying")
			if _, err := Step(c, "b3", func(StepContext) (string, error) { return "b3", nil }); err != nil {
				return "", err
			}
			c.Logger().Info("b-live")
			return "b", nil
		})
		if _, err := a.Result(); err != nil {
			return "", err
		}
		if _, err := b.Result(); err != nil {
			return "", err
		}
		return "ok", nil
	}, withLambdaAPI(fake), WithLogger(NewWriterLogger(&out)))

	resp, err := h(t.Context(), payload)
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if !strings.Contains(string(resp), `"SUCCEEDED"`) {
		t.Fatalf("response = %s, want SUCCEEDED", resp)
	}

	got := out.String()
	for _, hidden := range []string{"a-replaying", "b-still-replaying"} {
		if strings.Contains(got, hidden) {
			t.Errorf("replaying branch line %q must be suppressed, got:\n%s", hidden, got)
		}
	}
	for _, shown := range []string{"a-live", "b-live"} {
		if !strings.Contains(got, shown) {
			t.Errorf("live branch line %q must be emitted, got:\n%s", shown, got)
		}
	}
}
