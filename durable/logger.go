package durable

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Field names of the records the default log handler emits. They match the
// names the other durable execution SDKs use, so one CloudWatch query works
// across languages.
const (
	logKeyTimestamp     = "timestamp"
	logKeyMessage       = "message"
	logKeyRequestID     = "requestId"
	logKeyExecutionArn  = "executionArn"
	logKeyTenantID      = "tenantId"
	logKeyOperationID   = "operationId"
	logKeyOperationName = "operationName"
	logKeyAttempt       = "attempt"
	logKeyErrorType     = "errorType"
	logKeyErrorMessage  = "errorMessage"
	logKeyStackTrace    = "stackTrace"
)

// logLevelEnvVar is the environment variable that selects the default
// handler's minimum level. Lambda sets it from the function's logging
// configuration.
const logLevelEnvVar = "AWS_LAMBDA_LOG_LEVEL"

// logTimestampLayout renders a record time as ISO 8601 UTC with
// millisecond precision and a Z suffix, e.g. 2026-09-17T04:56:45.657Z.
const logTimestampLayout = "2006-01-02T15:04:05.000Z"

// newDefaultLogHandler returns the handler [Wrap] installs when no
// [WithLogHandler] option is given: a JSON handler writing to w whose
// records carry timestamp, level, and message under those names. An
// attribute whose value is an error is expanded into errorType and
// errorMessage; stackTrace is added when the error carries recorded
// frames. level is the minimum level; records below it are dropped before
// any formatting.
func newDefaultLogHandler(w io.Writer, level slog.Leveler) slog.Handler {
	return slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceDefaultAttr,
	})
}

// replaceDefaultAttr renames the built-in time and message keys, formats
// the time, and expands error values. It is called for every attribute at
// every group depth; the renames apply only at the top level, where the
// built-in keys live.
func replaceDefaultAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 {
		switch a.Key {
		case slog.TimeKey:
			if a.Value.Kind() == slog.KindTime {
				return slog.String(logKeyTimestamp, a.Value.Time().UTC().Format(logTimestampLayout))
			}
			return a
		case slog.MessageKey:
			a.Key = logKeyMessage
			return a
		}
	}
	if a.Value.Kind() == slog.KindAny {
		if err, ok := a.Value.Any().(error); ok {
			return errorAttrs(err)
		}
	}
	return a
}

// errorAttrs expands err into errorType and errorMessage, plus stackTrace
// when the error carries recorded frames. The result is a group with an
// empty key, which the JSON handler inlines into the enclosing object.
func errorAttrs(err error) slog.Attr {
	attrs := []any{
		slog.String(logKeyErrorType, errorTypeName(err)),
		slog.String(logKeyErrorMessage, err.Error()),
	}
	if frames := errorStackTrace(err); len(frames) > 0 {
		attrs = append(attrs, slog.Any(logKeyStackTrace, frames))
	}
	return slog.Group("", attrs...)
}

// errorStackTrace returns the frames recorded on err, if the SDK recorded
// any. Only the SDK's own failure types carry frames.
func errorStackTrace(err error) []string {
	switch e := err.(type) { //nolint:errorlint // the recorded frames belong to this value, not to a wrapped cause
	case *OperationError:
		return e.StackTrace
	case *StepError:
		return e.StackTrace
	case *ChildContextError:
		return e.StackTrace
	}
	return nil
}

// logLevelFromEnv maps the AWS_LAMBDA_LOG_LEVEL value to the default
// handler's minimum level. The comparison ignores case. An unset or
// unrecognised value selects INFO.
func logLevelFromEnv(value string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "TRACE":
		return slog.LevelDebug - 4
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	case "FATAL":
		return slog.LevelError + 4
	default:
		return slog.LevelInfo
	}
}

// defaultLogHandler builds the handler used when no [WithLogHandler] option
// is given: JSON to stderr, Lambda's log channel, at the level
// AWS_LAMBDA_LOG_LEVEL selects.
func defaultLogHandler() slog.Handler {
	return newDefaultLogHandler(os.Stderr, logLevelFromEnv(os.Getenv(logLevelEnvVar)))
}

// replayHandler wraps an [slog.Handler] with per-context replay
// suppression. It reports every level disabled, and drops every record,
// while replaying reports true. Because Enabled is consulted before a
// record is built, a suppressed call incurs no formatting cost.
//
// Suppression is decided per emitting context, not per invocation. Each
// context (root, child, and branch) owns one replayHandler whose replaying
// func reads that context's own mode. A branch that is still replaying
// therefore stays suppressed while a sibling branch that has reached live
// execution logs normally. The wrapper applies to whichever handler the
// user supplied, not only the default.
type replayHandler struct {
	inner     slog.Handler
	replaying func() bool
}

var _ slog.Handler = (*replayHandler)(nil)

// newReplayLogger returns a logger over inner whose records are dropped
// while replaying reports true.
func newReplayLogger(inner slog.Handler, replaying func() bool) *slog.Logger {
	return slog.New(&replayHandler{inner: inner, replaying: replaying})
}

func (h *replayHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.replaying() {
		return false
	}
	return h.inner.Enabled(ctx, level)
}

func (h *replayHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.replaying() {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

func (h *replayHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &replayHandler{inner: h.inner.WithAttrs(attrs), replaying: h.replaying}
}

func (h *replayHandler) WithGroup(name string) slog.Handler {
	return &replayHandler{inner: h.inner.WithGroup(name), replaying: h.replaying}
}

// executionLogAttrs are the attributes every record of one invocation
// carries: the request ID and execution ARN always, and the tenant ID when
// the invocation has one.
func executionLogAttrs(executionArn string, inv invocationInfo) []slog.Attr {
	attrs := []slog.Attr{
		slog.String(logKeyRequestID, inv.requestID),
		slog.String(logKeyExecutionArn, executionArn),
	}
	if inv.tenantID != "" {
		attrs = append(attrs, slog.String(logKeyTenantID, inv.tenantID))
	}
	return attrs
}

// contextLogAttrs are the attributes a child context adds to its records:
// the child operation's wire ID and its name when it has one. A child
// context has no attempt number; only a step body, condition check, or
// callback submitter carries one.
func contextLogAttrs(id, name string) []slog.Attr {
	attrs := []slog.Attr{slog.String(logKeyOperationID, hashID(id))}
	if name != "" {
		attrs = append(attrs, slog.String(logKeyOperationName, name))
	}
	return attrs
}

// operationLogAttrs are the attributes a step body, condition check, or
// callback submitter carries on its records: the operation's wire ID, its
// name when it has one, and the attempt number.
func operationLogAttrs(id, name string, attempt int) []slog.Attr {
	return append(contextLogAttrs(id, name), slog.Int(logKeyAttempt, attempt))
}

// errorTypeName returns the type name recorded for err in log records: the
// error type an SDK failure recorded, else the error's Go type name as
// [userErrorTypeName] derives it.
func errorTypeName(err error) string {
	switch e := err.(type) { //nolint:errorlint // the recorded type belongs to this value, not to a wrapped cause
	case *OperationError:
		if e.ErrorType != "" {
			return e.ErrorType
		}
	case *StepError:
		if e.ErrorType != "" {
			return e.ErrorType
		}
	case *ChildContextError:
		if e.ErrorType != "" {
			return e.ErrorType
		}
	}
	return userErrorTypeName(err)
}
