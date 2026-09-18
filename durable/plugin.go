package durable

import (
	"context"
	"sync"
	"time"
)

// Plugin configures an EXPERIMENTAL instrumentation plugin that observes
// durable execution lifecycle events. Plugins implement only the hooks they
// need by setting non-nil function fields; nil fields are skipped with zero
// overhead.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases without a major-version bump.
//
// Goroutine safety: hook dispatches from concurrent branches (parallel map
// items, async operations) may run in parallel. Plugin implementations must
// be safe for concurrent use from multiple goroutines.
type Plugin struct {
	// OnInvocationStart is called once at the start of each Lambda
	// invocation, before the user handler runs.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnInvocationStart func(ctx context.Context, info InvocationHookInfo)

	// OnInvocationEnd is called once when the invocation ends, including
	// with status PENDING on suspension.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnInvocationEnd func(ctx context.Context, info InvocationEndHookInfo)

	// OnOperationStart is called when a durable operation begins
	// execution. Fires on replayed operations with IsReplay=true.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnOperationStart func(ctx context.Context, info OperationHookInfo)

	// OnOperationEnd is called when a durable operation completes. Fires
	// on replayed operations with IsReplay=true.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnOperationEnd func(ctx context.Context, info OperationHookInfo)

	// OnOperationAttemptStart is called before each attempt of a
	// retryable operation (Step, WaitForCondition).
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnOperationAttemptStart func(ctx context.Context, info AttemptHookInfo)

	// OnOperationAttemptEnd is called after each attempt of a retryable
	// operation completes.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnOperationAttemptEnd func(ctx context.Context, info AttemptEndHookInfo)

	// OnOperationChange is called at invocation start for operations
	// whose status changed externally between invocations.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnOperationChange func(ctx context.Context, info OperationChangeHookInfo)

	// WrapInvocation wraps the user handler invocation. The outer plugin
	// (index 0) wraps first. fn must be called exactly once.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	WrapInvocation func(ctx context.Context, info InvocationHookInfo, fn func() (any, error)) (any, error)

	// WrapOperationAttemptFn wraps the execution of an operation attempt
	// body (step fn, condition check). fn must be called exactly once.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	WrapOperationAttemptFn func(ctx context.Context, info AttemptHookInfo, fn func() (any, error)) (any, error)

	// WrapChildContextFn wraps the execution of a child-context function.
	// fn must be called exactly once.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	WrapChildContextFn func(ctx context.Context, info OperationHookInfo, fn func() (any, error)) (any, error)

	// EnrichLogContext returns additional key-value pairs to merge into
	// every log record emitted through [Context.Logger] and
	// [StepContext.Logger]. It is called once per record, after replay
	// suppression, so a record dropped during replay does not invoke it.
	// ctx is the record's context: the one passed to a *Context logging
	// method such as [slog.Logger.InfoContext], else context.Background().
	//
	// Each returned entry becomes an attribute of the record unless its
	// key is already taken. Precedence, highest first: the SDK's own
	// fields (timestamp, level, message, requestId, executionArn,
	// tenantId, operationId, operationName, attempt), then attributes the
	// user supplied with the record or through [slog.Logger.With], then
	// plugin fields. A plugin field under a taken key is dropped, so a
	// plugin cannot overwrite the SDK's identifiers. Keys are compared by
	// qualified path: a plugin field lands under the groups the logger has
	// opened with [slog.Logger.WithGroup], and collides only with an
	// attribute at that same path, with the children of an empty-key group
	// counting at the enclosing path. The SDK's own fields are top-level,
	// so under an open group a plugin field may use their names. A group
	// the logger opens after an attribute was attached at that same path
	// is also taken: the plugin fields would form a second object under
	// that key beside the attached one, so none are added to records
	// logged through that logger. When
	// several plugins implement the hook, their maps are merged in
	// registration order and a later plugin's value replaces an earlier
	// one's under the same key. Fields are added in key order. A hook that
	// panics contributes no fields and does not fail the log call or the
	// invocation. When no plugin implements the hook, no per-record work
	// is done.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	EnrichLogContext func(ctx context.Context) map[string]any
}

// InvocationHookInfo carries context for invocation-level hooks.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type InvocationHookInfo struct {
	ExecutionArn      string
	IsFirstInvocation bool

	// ExecutionInput is the deserialized customer event for the execution.
	// It is the raw unmarshaled value (typically a map or struct).
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	ExecutionInput any

	// ExecutionStartTimestamp is the time the execution was first created,
	// sourced from the execution operation's StartTimestamp in the wire
	// payload. Zero when unavailable (e.g. payload lacks timestamp data).
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	ExecutionStartTimestamp time.Time

	// UpdatedOperations contains operations whose status changed
	// externally between invocations, keyed by operation ID. This
	// embeds the same data as OnOperationChange to allow plugins that
	// need both to avoid state ordering dependencies.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	UpdatedOperations map[string]OperationHookInfo
}

// InvocationEndHookInfo carries context for the OnInvocationEnd hook.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type InvocationEndHookInfo struct {
	ExecutionArn string
	Status       PluginInvocationStatus

	// ExecutionResult is the handler's return value when the invocation
	// succeeded. Nil on failure or suspension.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	ExecutionResult any

	// ExecutionError is the error that caused invocation failure. Nil on
	// success or suspension.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	ExecutionError error
}

// PluginInvocationStatus is the invocation outcome visible to plugins.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type PluginInvocationStatus string

// Invocation status constants for plugin hooks.
//
// EXPERIMENTAL: these constants are experimental and may be changed or
// removed in future releases.
const (
	PluginInvocationSucceeded PluginInvocationStatus = "SUCCEEDED"
	PluginInvocationFailed    PluginInvocationStatus = "FAILED"
	PluginInvocationPending   PluginInvocationStatus = "PENDING"
)

// OperationHookInfo carries context for operation-level hooks.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type OperationHookInfo struct {
	ExecutionArn string
	ID           string
	Name         string
	Type         string
	SubType      string
	Status       PluginOperationStatus
	Attempt      int
	IsReplay     bool

	// ParentID is the ID of the parent context operation, if any. Empty
	// for top-level (root context) operations. Used by insight to filter
	// and build the operation tree.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	ParentID string

	// StartTimestamp is when this operation began. Set on OnOperationStart
	// and OnOperationEnd; zero on hooks where not yet known.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	StartTimestamp time.Time

	// EndTimestamp is when this operation reached a terminal state. Set on
	// OnOperationEnd; zero on OnOperationStart.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	EndTimestamp time.Time

	// Result is the operation's serialized result (raw wire form), if any.
	// Set on OnOperationEnd for succeeded operations; empty otherwise.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	Result string

	// Error is the error the operation failed with, if any. Set on
	// OnOperationEnd for failed operations; nil otherwise.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	Error error
}

// PluginOperationStatus is an operation's lifecycle status visible to
// plugins.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type PluginOperationStatus string

// Operation status constants for plugin hooks.
//
// EXPERIMENTAL: these constants are experimental and may be changed or
// removed in future releases.
const (
	PluginOperationStarted   PluginOperationStatus = "STARTED"
	PluginOperationReady     PluginOperationStatus = "READY"
	PluginOperationPending   PluginOperationStatus = "PENDING"
	PluginOperationSucceeded PluginOperationStatus = "SUCCEEDED"
	PluginOperationFailed    PluginOperationStatus = "FAILED"
	PluginOperationTimedOut  PluginOperationStatus = "TIMED_OUT"
	PluginOperationStopped   PluginOperationStatus = "STOPPED"
	PluginOperationCancelled PluginOperationStatus = "CANCELLED"
)

// AttemptHookInfo carries context for attempt-level hooks.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type AttemptHookInfo struct {
	OperationHookInfo
	Attempt int
}

// AttemptEndHookInfo carries context for the OnOperationAttemptEnd hook.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type AttemptEndHookInfo struct {
	OperationHookInfo
	Attempt int
	Outcome PluginAttemptOutcome
	Error   error
}

// PluginAttemptOutcome is the result of an operation attempt.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type PluginAttemptOutcome string

// Attempt outcome constants for plugin hooks.
//
// EXPERIMENTAL: these constants are experimental and may be changed or
// removed in future releases.
const (
	PluginAttemptSucceeded PluginAttemptOutcome = "SUCCEEDED"
	PluginAttemptFailed    PluginAttemptOutcome = "FAILED"
)

// OperationChangeHookInfo carries context for OnOperationChange.
//
// EXPERIMENTAL: this type is experimental and may be changed or removed in
// future releases.
type OperationChangeHookInfo struct {
	ExecutionArn      string
	UpdatedOperations map[string]OperationHookInfo
}

// WithPlugins registers instrumentation plugins with the handler.
//
// EXPERIMENTAL: this function is experimental and may be changed or removed
// in future releases.
func WithPlugins(plugins ...Plugin) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.plugins = append(o.plugins, plugins...)
	})
}

// pluginDispatcher dispatches lifecycle hooks to registered plugins
// concurrently. It implements the 7-point dispatch contract from the spec.
//
// When no plugins are registered, all methods are no-ops with zero
// allocation.
type pluginDispatcher struct {
	plugins []Plugin
}

// newPluginDispatcher creates a dispatcher from the handler's registered
// plugins. Returns nil when no plugins are registered; callers should use
// the package-level dispatch functions which handle nil.
func newPluginDispatcher(plugins []Plugin) *pluginDispatcher {
	if len(plugins) == 0 {
		return nil
	}
	return &pluginDispatcher{plugins: plugins}
}

// dispatchNotification fans out to every plugin's hook concurrently (one
// goroutine per plugin) and waits for all to complete. Panics and errors
// inside hook functions are swallowed (recovered) and never affect execution.
//
// No-op when d is nil or has no plugins (zero overhead fast path).
func dispatchNotification(d *pluginDispatcher, call func(*Plugin)) {
	if d == nil {
		return
	}
	if len(d.plugins) == 1 {
		invokePluginSafely(&d.plugins[0], call)
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(d.plugins))
	for i := range d.plugins {
		p := &d.plugins[i]
		go func() {
			defer wg.Done()
			invokePluginSafely(p, call)
		}()
	}
	wg.Wait()
}

// invokePluginSafely calls call(p), recovering any panic so a misbehaving
// plugin never affects execution.
func invokePluginSafely(p *Plugin, call func(*Plugin)) {
	defer func() { _ = recover() }()
	call(p)
}

// wrapChain composes plugins' wrap hooks around fn. plugins[0] is outermost.
// A panicking wrapper is skipped in favor of the inner fn. Returns fn()
// unchanged when d is nil.
//
// getWrap extracts the wrap function from a plugin; return nil if the
// plugin does not implement this particular wrap hook.
func wrapChain(d *pluginDispatcher, getWrap func(*Plugin) func(func() (any, error)) (any, error), fn func() (any, error)) (any, error) {
	if d == nil {
		return fn()
	}

	// Build chain: plugins[0] outermost via reduceRight
	next := fn
	for i := len(d.plugins) - 1; i >= 0; i-- {
		wrap := getWrap(&d.plugins[i])
		if wrap == nil {
			continue
		}
		innerNext := next
		wrapFn := wrap
		next = func() (any, error) {
			return invokeWrapSafely(wrapFn, innerNext)
		}
	}
	return next()
}

// invokeWrapSafely calls wrapFn(innerFn), recovering panics. If the
// wrapper panics, the inner fn is called directly (panicking wrappers are
// skipped in favor of the inner fn).
func invokeWrapSafely(wrapFn func(func() (any, error)) (any, error), innerFn func() (any, error)) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Wrapper panicked; skip it and call inner fn directly.
			result, err = innerFn()
		}
	}()
	return wrapFn(innerFn)
}

// enrichLogContext merges all plugins' log context enrichments in
// registration order, a later plugin's value replacing an earlier one's
// under the same key. Returns nil when d is nil or no plugin provides
// context.
func enrichLogContext(ctx context.Context, d *pluginDispatcher) map[string]any {
	if d == nil {
		return nil
	}
	var merged map[string]any
	for i := range d.plugins {
		fn := d.plugins[i].EnrichLogContext
		if fn == nil {
			continue
		}
		m := safeEnrichLogContext(ctx, fn)
		if m == nil {
			continue
		}
		if merged == nil {
			merged = make(map[string]any, len(m))
		}
		for k, v := range m {
			merged[k] = v
		}
	}
	return merged
}

// safeEnrichLogContext calls fn with panic recovery. A panicking hook
// contributes no fields and does not fail the log call or the invocation.
func safeEnrichLogContext(ctx context.Context, fn func(context.Context) map[string]any) (result map[string]any) {
	defer func() { _ = recover() }()
	return fn(ctx)
}

// hasLogEnricher reports whether at least one registered plugin implements
// EnrichLogContext. False for a nil dispatcher.
func (d *pluginDispatcher) hasLogEnricher() bool {
	if d == nil {
		return false
	}
	for i := range d.plugins {
		if d.plugins[i].EnrichLogContext != nil {
			return true
		}
	}
	return false
}

// toPluginOperationStatus converts an internal operation status to the
// plugin-visible status type.
func toPluginOperationStatus(s operationStatus) PluginOperationStatus {
	switch s {
	case statusStarted:
		return PluginOperationStarted
	case statusReady:
		return PluginOperationReady
	case statusPending:
		return PluginOperationPending
	case statusSucceeded:
		return PluginOperationSucceeded
	case statusFailed:
		return PluginOperationFailed
	case statusTimedOut:
		return PluginOperationTimedOut
	case statusStopped:
		return PluginOperationStopped
	case statusCancelled:
		return PluginOperationCancelled
	default:
		return PluginOperationStatus(s)
	}
}
