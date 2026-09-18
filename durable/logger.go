package durable

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"
)

// defaultLogger is a structured logger that outputs JSON records to stderr
// (Lambda's log channel), enriched with the execution ARN. Replay
// suppression is not decided here: each context wraps its logger in a
// replayAwareLogger that consults that context's own replay state.
type defaultLogger struct {
	inner *slog.Logger
}

var _ Logger = (*defaultLogger)(nil)

func newDefaultLogger(executionArn string) *defaultLogger {
	return &defaultLogger{
		inner: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
				if a.Key == slog.MessageKey {
					a.Key = "message"
				}
				return a
			},
		})).With("executionArn", executionArn),
	}
}

func (l *defaultLogger) Debug(msg string, fields ...any) { l.inner.Debug(msg, fields...) }
func (l *defaultLogger) Info(msg string, fields ...any)  { l.inner.Info(msg, fields...) }
func (l *defaultLogger) Warn(msg string, fields ...any)  { l.inner.Warn(msg, fields...) }
func (l *defaultLogger) Error(msg string, fields ...any) { l.inner.Error(msg, fields...) }

// replayAwareLogger wraps a [Logger] with per-context replay suppression.
// It delegates to the inner logger only when replaying reports false.
//
// Suppression is decided per emitting context, not per invocation. Each
// context (root, child, and branch) owns one replayAwareLogger whose
// replaying func reads that context's own mode. A branch that is still
// replaying therefore stays suppressed while a sibling branch that has
// reached live execution logs normally.
type replayAwareLogger struct {
	inner     Logger
	replaying func() bool
}

var _ Logger = (*replayAwareLogger)(nil)

// newReplayAwareLogger wraps inner so that records are dropped while
// replaying reports true.
func newReplayAwareLogger(inner Logger, replaying func() bool) *replayAwareLogger {
	return &replayAwareLogger{inner: inner, replaying: replaying}
}

func (l *replayAwareLogger) Debug(msg string, fields ...any) {
	if l.replaying() {
		return
	}
	l.inner.Debug(msg, fields...)
}

func (l *replayAwareLogger) Info(msg string, fields ...any) {
	if l.replaying() {
		return
	}
	l.inner.Info(msg, fields...)
}

func (l *replayAwareLogger) Warn(msg string, fields ...any) {
	if l.replaying() {
		return
	}
	l.inner.Warn(msg, fields...)
}

func (l *replayAwareLogger) Error(msg string, fields ...any) {
	if l.replaying() {
		return
	}
	l.inner.Error(msg, fields...)
}

// NopLogger is a [Logger] that discards all output. Useful for tests or
// when logging is unwanted.
type NopLogger struct{}

var _ Logger = NopLogger{}

func (NopLogger) Debug(_ string, _ ...any) {}
func (NopLogger) Info(_ string, _ ...any)  {}
func (NopLogger) Warn(_ string, _ ...any)  {}
func (NopLogger) Error(_ string, _ ...any) {}

// WriterLogger writes structured JSON log lines to w. It is useful for
// tests that want to capture log output.
type WriterLogger struct {
	w io.Writer
}

var _ Logger = (*WriterLogger)(nil)

// NewWriterLogger creates a [Logger] that writes structured JSON to w.
func NewWriterLogger(w io.Writer) *WriterLogger {
	return &WriterLogger{w: w}
}

func (l *WriterLogger) Debug(msg string, fields ...any) { l.emit("DEBUG", msg, fields) }
func (l *WriterLogger) Info(msg string, fields ...any)  { l.emit("INFO", msg, fields) }
func (l *WriterLogger) Warn(msg string, fields ...any)  { l.emit("WARN", msg, fields) }
func (l *WriterLogger) Error(msg string, fields ...any) { l.emit("ERROR", msg, fields) }

func (l *WriterLogger) emit(level, msg string, fields []any) {
	record := map[string]any{
		"level":     level,
		"message":   msg,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			key = fmt.Sprint(fields[i])
		}
		record[key] = fields[i+1]
	}
	b, _ := json.Marshal(record)
	b = append(b, '\n')
	_, _ = l.w.Write(b)
}
