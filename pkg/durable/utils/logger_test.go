package utils

import (
	"testing"
)

// recordingBaseLogger is a minimal types.Logger test double for unit
// testing ContextLogger in isolation from the rest of the SDK (the
// higher-level replay-skip/contextual-field behavior is additionally
// covered end-to-end in pkg/durable/durable_logging_test.go's
// recordingLogger-based tests; this file unit-tests ContextLogger's own
// field-merging/suppression logic directly).
type recordingBaseLogger struct {
	calls []struct {
		level  string
		msg    string
		fields map[string]any
	}
}

func (l *recordingBaseLogger) Debug(msg string, fields map[string]any) {
	l.record("DEBUG", msg, fields)
}
func (l *recordingBaseLogger) Info(msg string, fields map[string]any) { l.record("INFO", msg, fields) }
func (l *recordingBaseLogger) Warn(msg string, fields map[string]any) { l.record("WARN", msg, fields) }
func (l *recordingBaseLogger) Error(msg string, fields map[string]any) {
	l.record("ERROR", msg, fields)
}

func (l *recordingBaseLogger) record(level, msg string, fields map[string]any) {
	l.calls = append(l.calls, struct {
		level  string
		msg    string
		fields map[string]any
	}{level, msg, fields})
}

func TestContextLogger_MergesFields(t *testing.T) {
	base := &recordingBaseLogger{}
	cl := NewContextLogger(base, map[string]any{"executionArn": "arn:1"}, false, nil)

	cl.Info("hello", map[string]any{"extra": "value"})

	if len(base.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(base.calls))
	}
	got := base.calls[0].fields
	if got["executionArn"] != "arn:1" {
		t.Errorf("expected executionArn=arn:1, got %v", got["executionArn"])
	}
	if got["extra"] != "value" {
		t.Errorf("expected extra=value, got %v", got["extra"])
	}
}

func TestContextLogger_CallerFieldsTakePrecedenceOverContextual(t *testing.T) {
	base := &recordingBaseLogger{}
	cl := NewContextLogger(base, map[string]any{"attempt": 1}, false, nil)

	cl.Info("hello", map[string]any{"attempt": 99})

	got := base.calls[0].fields
	if got["attempt"] != 99 {
		t.Errorf("expected caller-supplied attempt=99 to win, got %v", got["attempt"])
	}
}

func TestContextLogger_ModeAwareSuppression(t *testing.T) {
	base := &recordingBaseLogger{}
	suppressed := true
	cl := NewContextLogger(base, nil, true, func() bool { return suppressed })

	cl.Info("suppressed call", nil)
	if len(base.calls) != 0 {
		t.Fatalf("expected the call to be suppressed, got %d calls", len(base.calls))
	}

	suppressed = false
	cl.Info("not suppressed call", nil)
	if len(base.calls) != 1 {
		t.Fatalf("expected exactly 1 call once suppressed() flips false, got %d", len(base.calls))
	}
	if base.calls[0].msg != "not suppressed call" {
		t.Errorf("expected 'not suppressed call', got %q", base.calls[0].msg)
	}
}

func TestContextLogger_ModeAwareFalseNeverSuppresses(t *testing.T) {
	base := &recordingBaseLogger{}
	cl := NewContextLogger(base, nil, false, func() bool { return true })

	cl.Info("always logs", nil)
	if len(base.calls) != 1 {
		t.Fatalf("expected the call to log despite suppressed()==true, since modeAware is false, got %d calls", len(base.calls))
	}
}

func TestContextLogger_WithFieldsLayersOnTopWithoutMutatingOriginal(t *testing.T) {
	base := &recordingBaseLogger{}
	root := NewContextLogger(base, map[string]any{"executionArn": "arn:1"}, false, nil)
	child := root.WithFields(map[string]any{"operationId": "1"})

	child.Info("from child", nil)
	root.Info("from root", nil)

	if len(base.calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(base.calls))
	}
	childFields := base.calls[0].fields
	if childFields["executionArn"] != "arn:1" || childFields["operationId"] != "1" {
		t.Errorf("expected child call to carry both fields, got %v", childFields)
	}
	rootFields := base.calls[1].fields
	if _, ok := rootFields["operationId"]; ok {
		t.Errorf("expected root logger to remain unaffected by WithFields, got %v", rootFields)
	}
}

func TestContextLogger_NilBaseDefaultsToDefaultLogger(t *testing.T) {
	cl := NewContextLogger(nil, nil, false, nil)
	if _, ok := cl.base.(DefaultLogger); !ok {
		t.Errorf("expected nil base to default to DefaultLogger, got %T", cl.base)
	}
}
