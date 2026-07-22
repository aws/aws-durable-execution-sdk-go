package utils

import (
	"fmt"
	"log"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// DefaultLogger is a minimal types.Logger backed by the standard library
// "log" package. Suitable for local development; production use should
// typically supply a structured logger via types.LoggerConfig.CustomLogger.
type DefaultLogger struct{}

func (DefaultLogger) Debug(msg string, fields map[string]any) { logWithFields("DEBUG", msg, fields) }
func (DefaultLogger) Info(msg string, fields map[string]any)  { logWithFields("INFO", msg, fields) }
func (DefaultLogger) Warn(msg string, fields map[string]any)  { logWithFields("WARN", msg, fields) }
func (DefaultLogger) Error(msg string, fields map[string]any) { logWithFields("ERROR", msg, fields) }

func logWithFields(level, msg string, fields map[string]any) {
	if len(fields) == 0 {
		log.Printf("[%s] %s", level, msg)
		return
	}
	log.Printf("[%s] %s %s", level, msg, fmt.Sprint(fields))
}

// NopLogger discards all log output. Useful as a default when logging is
// not desired (e.g. in unit tests).
type NopLogger struct{}

func (NopLogger) Debug(string, map[string]any) {}
func (NopLogger) Info(string, map[string]any)  {}
func (NopLogger) Warn(string, map[string]any)  {}
func (NopLogger) Error(string, map[string]any) {}

var (
	_ types.Logger = DefaultLogger{}
	_ types.Logger = NopLogger{}
)

// -----------------------------------------------------------------------
// ContextLogger — replay-mode-aware, contextually-scoped logger wrapper
// (docs/remaining-work.md §5 tasks 13/14)
// -----------------------------------------------------------------------
//
// # Design
//
// Java's MDC (Mapped Diagnostic Context) is thread-local mutable state:
// calling context.getLogger() populates a shared, goroutine/thread-scoped
// map that every subsequent log call on ANY logger implicitly reads from,
// until something clears it. JS's DurableLoggingContext is conceptually
// similar but scoped to a single-threaded event-loop "current execution"
// notion rather than an OS thread.
//
// Go has neither a natural thread-local-state primitive (goroutines are
// meant to be lightweight and anonymous - "goroutine-local storage" is a
// well-known anti-pattern) nor Java/JS's implicit single mutable context.
// But this SDK doesn't need to invent one: dcontext.Context (for
// DurableContext) and dcontext.stepCtx (for StepContext) are ALREADY a
// value-per-scope design - a root Context, each of its children (one per
// RunInChildContext/Map-item/Parallel-branch), and each step attempt's
// StepContext are already distinct Go values, each held only by the code
// that's logically "inside" that scope. That is exactly the same scoping
// MDC achieves via thread-locals, just made explicit as regular Go values
// instead of implicit ambient state. So the correct Go-idiomatic
// implementation is: wrap the user-configured base Logger once per scope
// with the fields that scope should carry, and hand out THAT wrapped
// value from Logger() - never mutate a shared logger in place, never
// reach for goroutine-local storage.
//
// ContextLogger is that wrapper. It is constructed by dcontext.Context
// (for the executionArn/child-context fields) and dcontext.NewStepContext
// (for the operationId/operationName/attempt fields) - see both of those
// for the call sites - and is intentionally NOT exported as something
// user code constructs directly, mirroring how JS/Java also don't expose
// their MDC/DurableLoggingContext plumbing as a public constructor.
type ContextLogger struct {
	base types.Logger

	// fields are merged into every call's fields map, with the call's own
	// fields taking precedence on key collision (a caller's explicit
	// field should never be silently clobbered by contextual metadata).
	fields map[string]any

	// modeAware mirrors types.LoggerConfig.ModeAware at the time this
	// logger was constructed (see that field's doc for the default-value
	// nuance). Captured by value, not by reference to a LoggerConfig,
	// since ConfigureLogger's later changes should only affect loggers
	// obtained AFTER the reconfiguration - matching JS/Java, where
	// configureLogger's effect is likewise not retroactive to loggers a
	// caller already captured into a local variable.
	modeAware bool

	// suppressed reports, at the moment each log call happens, whether
	// this call is currently occurring during a replay-skip (see this
	// package's doc and dcontext.Context's replayState for exactly what
	// that means) rather than real execution. It's a func(), not a bool
	// captured at construction time, specifically because a single
	// DurableContext-level logger obtained ONCE before any operations run
	// must still suppress differently across the SAME handler invocation
	// as it crosses from "still replaying already-completed operations"
	// into "reached the first real one" - see replayState's doc for why
	// this needs to be a shared, mutable-over-time flag rather than a
	// point-in-time snapshot.
	suppressed func() bool
}

// NewContextLogger wraps base with fields merged into every call and
// replay-mode-aware suppression gated by modeAware/suppressed. base
// defaults to DefaultLogger{} if nil.
//
// fields is copied defensively (not aliased) so a caller that mutates the
// map it passed in afterward cannot retroactively change an
// already-constructed logger's behavior.
func NewContextLogger(base types.Logger, fields map[string]any, modeAware bool, suppressed func() bool) *ContextLogger {
	if base == nil {
		base = DefaultLogger{}
	}
	if suppressed == nil {
		suppressed = func() bool { return false }
	}
	cp := make(map[string]any, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	return &ContextLogger{base: base, fields: cp, modeAware: modeAware, suppressed: suppressed}
}

// WithFields returns a new ContextLogger layering additional fields on top
// of l's own (l's fields still apply; extra takes precedence on key
// collision, since a more specific/nested scope's field name should win
// over an outer scope's), sharing l's base logger, modeAware setting, and
// suppressed function. Used by dcontext.NewStepContext to layer
// operationId/operationName/attempt on top of the enclosing DurableContext
// logger's own fields (e.g. executionArn, and contextId/parentId if this
// step is inside a child context).
func (l *ContextLogger) WithFields(extra map[string]any) *ContextLogger {
	merged := make(map[string]any, len(l.fields)+len(extra))
	for k, v := range l.fields {
		merged[k] = v
	}
	for k, v := range extra {
		merged[k] = v
	}
	return &ContextLogger{base: l.base, fields: merged, modeAware: l.modeAware, suppressed: l.suppressed}
}

func (l *ContextLogger) Debug(msg string, fields map[string]any) { l.log(l.base.Debug, msg, fields) }
func (l *ContextLogger) Info(msg string, fields map[string]any)  { l.log(l.base.Info, msg, fields) }
func (l *ContextLogger) Warn(msg string, fields map[string]any)  { l.log(l.base.Warn, msg, fields) }
func (l *ContextLogger) Error(msg string, fields map[string]any) { l.log(l.base.Error, msg, fields) }

func (l *ContextLogger) log(emit func(string, map[string]any), msg string, fields map[string]any) {
	if l.modeAware && l.suppressed() {
		return
	}
	if len(l.fields) == 0 {
		emit(msg, fields)
		return
	}
	merged := make(map[string]any, len(l.fields)+len(fields))
	for k, v := range l.fields {
		merged[k] = v
	}
	// Caller-supplied fields take precedence over contextual ones on key
	// collision - see this type's doc.
	for k, v := range fields {
		merged[k] = v
	}
	emit(msg, merged)
}

var _ types.Logger = (*ContextLogger)(nil)
