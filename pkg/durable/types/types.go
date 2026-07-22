// Package types defines the public types and interfaces shared across the
// AWS Durable Execution SDK for Go. It intentionally mirrors the operation
// set and execution semantics of the Java and TypeScript SDKs (checkpoint
// tokens, replay model, step identifiers) while exposing an idiomatic Go
// surface: context.Context-based cancellation, generics for typed results,
// and error return values instead of exceptions/promises.
package types

import "context"

// DurableContext is passed to a durable function's handler and to any
// child/map/parallel branch functions. It is the entry point for all
// durable operations (Step, Wait, Invoke, RunInChildContext, Map, Parallel,
// WaitForCondition, WaitForCallback, CreateCallback).
//
// DurableContext itself does not expose the operations as methods (see the
// operations package) to keep the operation set open to generic, typed
// functions such as Step[T]. This mirrors Go idioms like io.Reader-based
// composition rather than Java's DurableHandler<I,O> class hierarchy.
type DurableContext interface {
	// Context returns the underlying context.Context for cancellation,
	// deadlines, and passing to AWS SDK / HTTP calls from within steps.
	Context() context.Context

	// Logger returns the replay-aware, contextually-scoped logger for this
	// execution/context (docs/remaining-work.md §5 tasks 13/14). Every
	// call made through it automatically carries this context's
	// structured fields - at minimum executionArn; a child context
	// (RunInChildContext/Map/Parallel - see dcontext.Context.NewChild)
	// additionally carries contextId/contextName/parentId, matching the
	// confirmed cross-SDK "DurableContext (child)" execution-metadata
	// fields - and, when LoggerConfig.ModeAware is enabled (the default
	// when no LoggerConfig is configured at all - see that field's doc),
	// suppresses calls made while this invocation is still replay-skipping
	// through already-checkpointed operations rather than genuinely
	// executing for the first time.
	Logger() Logger

	// ConfigureLogger customizes logging behavior (e.g. suppressing logs
	// during replay).
	ConfigureLogger(cfg LoggerConfig)

	// ExecutionARN returns the ARN of the durable execution.
	ExecutionARN() string

	// NextStepID reserves and returns the next hierarchical step ID for
	// this context. Used internally by the operations package; exposed for
	// custom operation implementations.
	NextStepID() string

	// IsReplaying reports whether the current operation is being replayed
	// from a checkpoint (true) or executed for the first time (false).
	IsReplaying() bool
}

// StepContext is passed to callbacks running inside a Step, WaitForCallback,
// or WaitForCondition operation. It intentionally does NOT expose durable
// operations (Step, Wait, etc.) - those may only be called on a
// DurableContext - to make replay-safety boundaries explicit at the type
// level.
type StepContext interface {
	// Context returns the underlying context.Context for I/O calls
	// (AWS SDK clients, HTTP requests, etc.) made from within the step.
	Context() context.Context

	// Logger returns a logger scoped with step metadata (step ID, name,
	// and attempt number - docs/remaining-work.md §5 task 14, matching the
	// confirmed cross-SDK "Operation context" execution-metadata fields:
	// operationId, operationName, attempt). Since a StepContext only ever
	// exists inside a real (non-replay-skipped) execution of an
	// operation's body - see dcontext.NewStepContext's call sites, all of
	// which are on the "actually run fn" path, never the replay-skip
	// early-return path - every log call made through it is, by
	// construction, never subject to ModeAware suppression: there is no
	// such thing as a StepContext during a replay-skip.
	Logger() Logger

	// Attempt returns the current retry attempt number, starting at 1.
	Attempt() int
}

// Logger is the structured logging interface used throughout the SDK.
// Implementations should be safe to call during replay; the SDK
// deduplicates/suppresses log calls made during replay when ModeAware is
// enabled in LoggerConfig.
//
// A Logger obtained via DurableContext.Logger()/StepContext.Logger() (see
// dcontext.Context/dcontext.NewStepContext) is never a bare, user-supplied
// Logger directly - it is always wrapped by utils.ContextLogger, which (a)
// merges in this operation's contextual fields (execution ARN, step/context
// ID, name, attempt - see docs/remaining-work.md §5 task 14) on every call,
// and (b) applies replay-mode-aware suppression per LoggerConfig.ModeAware
// (task 13) before delegating to the underlying base Logger (the default
// DefaultLogger, or LoggerConfig.CustomLogger if set). Implementations of
// this interface only ever need to worry about actually emitting a log
// line; the SDK owns both of those cross-cutting concerns at the wrapping
// layer, matching the JS SDK's DurableLoggingContext and Java's MDC-based
// approach without needing Go goroutine-local state to do it (see
// ContextLogger's doc for why the per-Context/per-StepContext scoping this
// SDK already has is a sufficient substitute).
type Logger interface {
	Debug(msg string, fields map[string]any)
	Info(msg string, fields map[string]any)
	Warn(msg string, fields map[string]any)
	Error(msg string, fields map[string]any)
}

// LoggerConfig customizes SDK logging behavior.
type LoggerConfig struct {
	// CustomLogger, if set, replaces the SDK's default logger.
	CustomLogger Logger

	// ModeAware, if true, suppresses log calls made while the current
	// operation is replay-skipping (returning an already-checkpointed
	// result without re-running real work) rather than actually executing
	// for the first time or genuinely re-executing (e.g. a retry's real
	// re-run of a step body). Matches the JS SDK's modeAware option and
	// Java's LoggerConfig.suppressReplayLogs, confirmed against the
	// official SDK reference doc's "Replay log suppression" section: "it
	// runs your handler from the start until it reaches the next
	// incomplete operation. It does not re-emit log entries encountered
	// before that point... Logs inside a retrying step body always emit,
	// because the step has not completed yet."
	//
	// # Go-specific default note
	//
	// The JS/Java SDKs default this to true (suppression ON) even when no
	// LoggerConfig is supplied at all. Go's zero value for bool is false,
	// so a bare LoggerConfig{} literal (as opposed to omitting LoggerConfig
	// entirely) would otherwise silently mean "suppression off" - the
	// opposite of every other SDK's default and of this field's own doc
	// comment. To keep the documented default (true) while still
	// respecting an explicit ModeAware: false, the SDK treats "no
	// LoggerConfig provided to Config.LoggerConfig / DurableContext never
	// had ConfigureLogger called" as ModeAware-true, and otherwise takes
	// whatever value this field was set to at face value once a
	// LoggerConfig IS supplied (see durable.WithDurableExecution and
	// dcontext.Context.ConfigureLogger). This means an explicit
	// LoggerConfig{CustomLogger: x} (ModeAware left at its bool zero value,
	// false) DOES turn suppression off - if you want a custom logger AND
	// the default suppression behavior, set ModeAware: true explicitly.
	// This is a deliberate, documented Go-idiom tradeoff (no pointer/
	// "unset" sentinel on a simple exported bool) rather than a port of
	// the other SDKs' "always defaults true" semantics verbatim.
	ModeAware bool
}

// Duration is a replay-safe, serializable duration. Prefer this over
// time.Duration in durable operation signatures so that checkpointed values
// have a stable, language-agnostic wire representation consistent with the
// JS/Java/Python SDKs.
type Duration struct {
	Days    int
	Hours   int
	Minutes int
	Seconds int
}

// Serdes defines custom serialization for checkpointed values. The default
// implementation (see utils.JSONSerdes) uses JSON. Implement this interface
// to support alternate storage strategies (e.g. large payloads in S3).
type Serdes interface {
	Serialize(value any, entityID string, executionARN string) (string, error)
	Deserialize(pointer string, entityID string, executionARN string) (any, error)
}

// RetryDecision is returned by a retry strategy function to indicate
// whether a failed step should be retried and, if so, after what delay.
type RetryDecision struct {
	ShouldRetry bool
	Delay       *Duration
}

// StepSemantics controls checkpointing behavior relative to retries.
type StepSemantics int

const (
	// StepSemanticsAtLeastOncePerRetry executes the step body at least once
	// per retry attempt before checkpointing. Safe for idempotent
	// operations. This is the default.
	StepSemanticsAtLeastOncePerRetry StepSemantics = iota

	// StepSemanticsAtMostOncePerRetry checkpoints before executing the step
	// body, ensuring the step is attempted at most once per checkpoint.
	// Use for non-idempotent operations, typically combined with a
	// no-retry strategy.
	StepSemanticsAtMostOncePerRetry
)

// WaitStrategyResult was the scaffold's original design for
// WaitForCondition's polling control, before that operation's real
// implementation landed. NOT USED: the actual operations.WaitForCondition
// reuses RetryDecision instead (the same type Step's retry strategies
// return), since the confirmed real-backend flowchart documents
// WaitForCondition as structurally identical to Step's own retry loop,
// just triggered by "conditionMet=false" rather than an error - see
// operations.WithConditionRetryStrategy's doc for the reasoning. Retained
// here only to avoid a breaking removal of a public type; do not use for
// new code.
type WaitStrategyResult struct {
	ShouldContinue bool
	Delay          *Duration
}

// CompletionConfig controls early-exit behavior for Map and Parallel
// operations.
type CompletionConfig struct {
	// MinSuccessful, if set, completes the batch once this many branches
	// have succeeded, cancelling the remainder.
	MinSuccessful *int

	// ToleratedFailureCount, if set, fails the batch once more than this
	// many branches have failed.
	ToleratedFailureCount *int

	// ToleratedFailurePercentage, if set, fails the batch once the failure
	// rate exceeds this percentage (0-100).
	ToleratedFailurePercentage *float64
}
