// Package plugin defines an EXPERIMENTAL instrumentation-plugin interface
// for the AWS Durable Execution SDK for Go, ported from the JS reference
// SDK's own DurableInstrumentationPlugin (types/plugin.ts, gated
// `@experimental` there too - see that file's own doc for the full
// lifecycle-hook contract this package mirrors).
//
// # Experimental status
//
// Everything in this package is EXPERIMENTAL: the interface, the info
// structs, and the status enums may all change or be removed in a future
// release without a major-version bump, exactly matching the JS SDK's own
// `@experimental` annotation on the equivalent types. Do not depend on
// this package for anything load-bearing; it exists for observability/
// tracing integrations (OpenTelemetry, X-Ray, custom metrics) layered on
// top of the SDK's checkpoint/replay lifecycle, not as a general
// extensibility mechanism for durable-execution business logic itself.
//
// # Scope
//
// This is a full, Go-idiomatic port of the JS interface's entire hook
// surface:
//
//   - OnInvocationStart / WrapInvocation / OnInvocationEnd - once per
//     Lambda invocation (including replays), from
//     durable.WithDurableExecution.
//   - OnOperationStart / WrapChildContextFn / OnOperationEnd - per durable
//     operation (Step, Wait, CreateCallback, WaitForCallback,
//     WaitForCondition, Invoke, RunInChildContext, and each Map
//     item/Parallel branch), for genuinely-executed (non-replay-skipped)
//     operations. WaitForCallback/WaitForCondition are compositions built
//     on top of the lower-level primitives (RunInChildContext,
//     CreateCallback, Step) and therefore inherit their hook coverage
//     automatically, exactly like the JS SDK's own operation
//     implementations.
//   - OnOperationAttemptStart / WrapOperationAttemptFn /
//     OnOperationAttemptEnd - per retry/poll attempt of a retryable
//     operation (currently: Step and WaitForCondition, which is
//     structurally the same per-attempt state machine as Step with a
//     different termination predicate - see operations.WaitForCondition's
//     own doc).
//   - OnOperationChange - fired once per invocation, for operations whose
//     status changed due to something external to the current call stack
//     becoming visible at invocation start (a wait timer elapsing, a
//     callback being resolved, a chained invoke completing).
//   - EnrichLogContext - merged into every log line emitted through a
//     DurableContext/StepContext logger.
//
// Deliberately NOT ported: PluginsConfig.ChildOperationsDepth (the
// companion knob controlling how much of the operation tree survives
// suspend/resume for plugins to inspect) - this requires backend/
// replay-state changes beyond the plugin dispatch mechanism itself and is
// tracked as separate follow-on work.
package plugin

import (
	"context"
	"sync"
	"time"
)

// InvocationStatus mirrors the JS SDK's PluginInvocationStatus: a richer
// status enumeration for plugin hooks than types.ExecutionStatus, since
// plugin authors care about a RETRYING distinction that the wire-level
// execution status does not surface.
type InvocationStatus string

const (
	InvocationStatusSucceeded InvocationStatus = "SUCCEEDED"
	InvocationStatusFailed    InvocationStatus = "FAILED"
	InvocationStatusPending   InvocationStatus = "PENDING"
	InvocationStatusRetrying  InvocationStatus = "RETRYING"
)

// OperationStatus mirrors the JS SDK's PluginOperationStatus: the
// lifecycle states an operation can be in, as surfaced to plugins.
type OperationStatus string

const (
	OperationStatusStarted   OperationStatus = "STARTED"
	OperationStatusReady     OperationStatus = "READY"
	OperationStatusPending   OperationStatus = "PENDING"
	OperationStatusSucceeded OperationStatus = "SUCCEEDED"
	OperationStatusFailed    OperationStatus = "FAILED"
	OperationStatusTimedOut  OperationStatus = "TIMED_OUT"
	OperationStatusStopped   OperationStatus = "STOPPED"
	OperationStatusCancelled OperationStatus = "CANCELLED"
)

// AttemptOutcome mirrors the JS SDK's AttemptEndInfoOutcome: the possible
// outcomes for a single operation attempt.
type AttemptOutcome string

const (
	AttemptOutcomeSucceeded AttemptOutcome = "SUCCEEDED"
	AttemptOutcomeFailed    AttemptOutcome = "FAILED"
)

// OperationInfo describes a durable operation at a point in its lifecycle,
// mirroring the JS SDK's OperationInfo.
type OperationInfo struct {
	ID       string
	Name     string
	Type     string
	SubType  string
	ParentID string
	Status   OperationStatus

	// StartTimestamp/EndTimestamp are set only on the corresponding
	// lifecycle hooks that have reached that point (e.g. EndTimestamp is
	// always zero on OnOperationStart).
	StartTimestamp time.Time
	EndTimestamp   time.Time

	// Result is the operation's checkpointed result, if any, as its raw
	// serialized wire form (matching the JS SDK's own OperationInfo.result,
	// which is likewise the raw string rather than a deserialized value -
	// deserializing here would require plugins to know the operation's
	// Serdes, which this dispatch layer does not have).
	Result string

	// Error is the error the operation failed with, if any, present
	// regardless of which lifecycle hook surfaced this info.
	Error error

	// Attempt is the current attempt number (1-based), when applicable
	// (e.g. steps). Zero when not applicable.
	Attempt int

	// IsReplay reports whether this operation's status was reconstructed
	// from a checkpoint made in a PRIOR invocation (true) or is being
	// observed live, in the invocation that is actually producing this
	// transition (false). OnOperationStart/OnOperationEnd are only ever
	// fired for genuinely-executed operations (never for a replay-skip
	// fast path that returns a checkpointed result without re-running
	// anything), so IsReplay is always false for operations reaching
	// either hook - the field exists for parity with the JS SDK's own
	// OperationInfo.isReplay and for OnOperationChange, which DOES
	// describe operations whose change happened in a prior invocation.
	IsReplay bool
}

// OperationEndInfo describes a durable operation that has just ended,
// mirroring the JS SDK's OperationEndInfo (which is OperationInfo plus a
// redundant, always-populated-together Error field in the JS types - kept
// here as a distinct type, rather than reusing OperationInfo directly, to
// match the JS SDK's own hook signatures one-for-one and leave room for
// end-specific fields in the future without an interface change).
type OperationEndInfo struct {
	OperationInfo
}

// AttemptInfo describes a single attempt of a retryable operation,
// mirroring the JS SDK's AttemptInfo.
type AttemptInfo struct {
	OperationInfo
	// Attempt is always populated for an AttemptInfo (unlike
	// OperationInfo.Attempt, which is zero when not applicable) - kept as
	// a promoted field via the embedded OperationInfo.Attempt rather than
	// a duplicate field, since Go has no interface-level "required field"
	// distinction to enforce this the way TypeScript's `attempt: number`
	// (vs OperationInfo's own `attempt?: number`) does.
}

// AttemptEndInfo describes a single attempt that has ended, mirroring the
// JS SDK's AttemptEndInfo.
type AttemptEndInfo struct {
	AttemptInfo
	Outcome AttemptOutcome
	Error   error
}

// InvocationInfo describes a single Lambda invocation of a durable
// execution, mirroring the JS SDK's InvocationInfo.
type InvocationInfo struct {
	RequestID         string
	ExecutionARN      string
	ExecutionInput    any
	IsFirstInvocation bool

	// ExecutionStartTimestamp is when the overall durable execution was
	// first started, as reported by the backend. This remains the same
	// across all invocations (including replays) of a given execution.
	ExecutionStartTimestamp time.Time

	// UpdatedOperations lists operations that changed externally between
	// the previous invocation and this one (e.g. a wait timer elapsed, a
	// callback was received, or a chained invoke completed), keyed by
	// operation ID. Empty on the first invocation. Mirrors the JS SDK's
	// InvocationInfo.updatedOperations.
	UpdatedOperations map[string]OperationInfo
}

// InvocationEndInfo describes a completed Lambda invocation, mirroring the
// JS SDK's InvocationEndInfo.
type InvocationEndInfo struct {
	RequestID    string
	ExecutionARN string
	Status       InvocationStatus

	// ExecutionResult is the handler's own returned result, present only
	// when Status is InvocationStatusSucceeded.
	ExecutionResult any

	// ExecutionError is the handler's own returned error, present only
	// when Status is InvocationStatusFailed.
	ExecutionError error
}

// OperationChangeInfo describes operations that changed status due to
// something external to the current call stack becoming visible at the
// start of an invocation, mirroring the JS SDK's OperationChangeInfo.
type OperationChangeInfo struct {
	ExecutionARN      string
	UpdatedOperations map[string]OperationInfo
}

// WrapFn is the function signature the three Wrap* hooks receive and must
// call exactly once, synchronously, returning its result/error unchanged.
// Modeled as `func() (any, error)` rather than a generic
// `func() (T, error)` because InstrumentationPlugin is a single,
// non-generic interface shared across every operation's differently-typed
// result - callers that need the concrete type assert on the returned
// any, mirroring how TypeScript's own CustomerFnResult is likewise
// `unknown`, not a generic parameter.
type WrapFn = func() (any, error)

// InstrumentationPlugin is the EXPERIMENTAL hook interface a caller
// implements to observe durable-execution lifecycle events, mirroring the
// JS SDK's DurableInstrumentationPlugin.
//
// # Await contract
//
// Every non-Wrap* hook in this interface is awaited (called synchronously
// and blocked on) by the SDK before execution proceeds past the
// corresponding lifecycle point, exactly matching the JS SDK's own
// documented contract. Hooks across multiple plugins are invoked
// concurrently (one goroutine per plugin per hook call), and the SDK
// waits for all of them to return before proceeding. A panic inside a
// hook is recovered/swallowed by the dispatch layer (see Dispatch) and
// never affects the execution outcome.
//
// Because hooks are awaited, any time spent in a hook directly increases
// the wall-clock duration of the current Lambda invocation. To avoid
// blocking, start background work inside a hook without waiting for it
// to finish (a plain Go goroutine launched from within the hook body),
// keeping in mind that Lambda freezes the execution environment once the
// invocation returns, so such detached work is best-effort only - not
// guaranteed to resume or complete, exactly like the JS SDK's own
// documented "fire-and-forget" caveat.
//
// # Optional hooks
//
// Go has no direct equivalent of TypeScript's optional interface methods,
// so InstrumentationPlugin embeds NoopPlugin's zero-value behavior by
// requiring implementers to embed *NoopPlugin (or NoopPlugin) rather than
// implementing every method - see NoopPlugin's own doc.
//
// # Wrap* hooks
//
// WrapInvocation, WrapChildContextFn, and WrapOperationAttemptFn are the
// three hooks that WRAP a function rather than merely observing a
// lifecycle point - the plugin receives the function to run (a WrapFn)
// and is responsible for calling it itself (typically via
// `defer`/recover-style instrumentation, e.g. an OpenTelemetry span that
// wraps the call). Unlike the JS SDK (single-threaded, Promise-based),
// this SDK's operations run on their own goroutines and coordinate
// suspension via execmgr.Manager - a Wrap* hook must call fn synchronously
// and return its result/error unchanged (not spawn it on a separate
// goroutine and return early), or it will break the active-goroutine
// accounting Register/Deregister depends on. When multiple plugins are
// configured, they are composed in order (the first plugin's wrapper
// becomes the outermost layer) via WrapChain - see that function's doc.
type InstrumentationPlugin interface {
	OnInvocationStart(ctx context.Context, info InvocationInfo)
	WrapInvocation(ctx context.Context, info InvocationInfo, fn WrapFn) (any, error)
	OnInvocationEnd(ctx context.Context, info InvocationEndInfo)

	OnOperationStart(ctx context.Context, info OperationInfo)
	WrapChildContextFn(ctx context.Context, info OperationInfo, fn WrapFn) (any, error)
	OnOperationEnd(ctx context.Context, info OperationEndInfo)

	OnOperationAttemptStart(ctx context.Context, info AttemptInfo)
	WrapOperationAttemptFn(ctx context.Context, info AttemptInfo, fn WrapFn) (any, error)
	OnOperationAttemptEnd(ctx context.Context, info AttemptEndInfo)

	OnOperationChange(ctx context.Context, info OperationChangeInfo)

	EnrichLogContext() map[string]any
}

// NoopPlugin implements every InstrumentationPlugin method as a no-op (the
// three Wrap* hooks simply call fn unchanged, i.e. they wrap nothing).
// Embed this in a concrete plugin type to only override the hooks you
// actually care about, e.g.:
//
//	type myPlugin struct {
//		plugin.NoopPlugin
//	}
//
//	func (p *myPlugin) OnOperationEnd(ctx context.Context, info plugin.OperationEndInfo) {
//		// only this hook is overridden; every other method uses NoopPlugin's
//		// embedded no-op implementation.
//	}
type NoopPlugin struct{}

func (NoopPlugin) OnInvocationStart(context.Context, InvocationInfo)  {}
func (NoopPlugin) OnInvocationEnd(context.Context, InvocationEndInfo) {}
func (NoopPlugin) WrapInvocation(_ context.Context, _ InvocationInfo, fn WrapFn) (any, error) {
	return fn()
}

func (NoopPlugin) OnOperationStart(context.Context, OperationInfo)  {}
func (NoopPlugin) OnOperationEnd(context.Context, OperationEndInfo) {}
func (NoopPlugin) WrapChildContextFn(_ context.Context, _ OperationInfo, fn WrapFn) (any, error) {
	return fn()
}

func (NoopPlugin) OnOperationAttemptStart(context.Context, AttemptInfo)  {}
func (NoopPlugin) OnOperationAttemptEnd(context.Context, AttemptEndInfo) {}
func (NoopPlugin) WrapOperationAttemptFn(_ context.Context, _ AttemptInfo, fn WrapFn) (any, error) {
	return fn()
}

func (NoopPlugin) OnOperationChange(context.Context, OperationChangeInfo) {}
func (NoopPlugin) EnrichLogContext() map[string]any                       { return nil }

// Dispatch fans out to every plugin's hook concurrently and blocks until
// all have returned, matching the JS SDK's documented "hooks across
// multiple plugins are invoked concurrently, and the SDK waits for all of
// them to settle" contract. call is invoked once per plugin, on its own
// goroutine; a panic inside any single call invocation is recovered and
// discarded so a misbehaving plugin can never affect the execution outcome
// or take down other plugins' invocations of the same hook.
//
// Dispatch is a no-op (returns immediately) when plugins is empty, so
// callers pay no synchronization cost when no plugins are configured -
// the common case. Use Dispatch for the non-Wrap* (pure notification)
// hooks; use WrapChain for the three Wrap* hooks, which need each
// plugin's own return value threaded into the next rather than a
// fire-and-forget fan-out.
func Dispatch(plugins []InstrumentationPlugin, call func(InstrumentationPlugin)) {
	if len(plugins) == 0 {
		return
	}
	if len(plugins) == 1 {
		invokeSafely(plugins[0], call)
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(plugins))
	for _, p := range plugins {
		p := p
		go func() {
			defer wg.Done()
			invokeSafely(p, call)
		}()
	}
	wg.Wait()
}

// invokeSafely calls call(p), recovering and discarding any panic so a
// single misbehaving plugin can never affect the execution outcome -
// mirroring the JS SDK's own "errors thrown by a hook are swallowed by
// the SDK and never affect the execution outcome" contract.
func invokeSafely(p InstrumentationPlugin, call func(InstrumentationPlugin)) {
	defer func() {
		_ = recover()
	}()
	call(p)
}

// WrapChain composes plugins' wrap function (e.g. a closure over
// InstrumentationPlugin.WrapChildContextFn) around fn, in order - the
// FIRST plugin in plugins becomes the OUTERMOST layer (it is called
// first, and its own call to the wrapped fn invokes the second plugin's
// wrapper, and so on), matching the JS SDK's own "plugins are applied in
// the order they appear in the array" composition rule.
//
// wrap is a closure that adapts a specific Wrap* hook (WrapInvocation,
// WrapChildContextFn, or WrapOperationAttemptFn) to this function's
// uniform `func(InstrumentationPlugin, WrapFn) (any, error)` shape - e.g.
// `func(p InstrumentationPlugin, next WrapFn) (any, error) { return
// p.WrapChildContextFn(ctx, info, next) }`.
//
// A panic inside any single plugin's wrap call is recovered and
// re-panicked with the original value AFTER unwinding back out of
// WrapChain (not swallowed, unlike Dispatch's pure-notification hooks):
// a Wrap* hook that panics instead of returning is far more likely to
// indicate a genuine control-flow bug in the plugin's own implementation
// (e.g. forgetting to call fn at all) than a swallowable instrumentation
// failure, and silently swallowing it would replace the real fn's own
// result with a plugin-caused nil/zero value - a much worse outcome than
// letting the panic propagate to this SDK's existing goroutine-level
// recovery (if any) exactly as an unwrapped fn's own panic would.
//
// Returns fn() unchanged (still exactly once) when plugins is empty.
func WrapChain(plugins []InstrumentationPlugin, wrap func(InstrumentationPlugin, WrapFn) (any, error), fn WrapFn) (any, error) {
	next := fn
	for i := len(plugins) - 1; i >= 0; i-- {
		p := plugins[i]
		innerNext := next
		next = func() (any, error) { return wrap(p, innerNext) }
	}
	return next()
}
