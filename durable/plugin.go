package durable

import (
	"context"
	"errors"
	"reflect"
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
//
// Wrap hooks: WrapInvocation, WrapOperationAttemptFn, and WrapChildContextFn
// receive the wrapped work as fn and must call fn exactly once, returning its
// result. fn takes a [context.Context]. The context a hook passes to fn
// becomes the parent of the context the wrapped user code observes: the
// handler's [Context] for WrapInvocation, the [StepContext] of a step body
// or condition check for WrapOperationAttemptFn, and the child [Context]
// for WrapChildContextFn. A hook that attaches a value to the ctx it
// received and passes the derived context to fn makes that value readable
// inside the user code, and inside every hook nested further in. A hook
// that passes the ctx it received unchanged causes no behavior change. A
// nil context is treated as the ctx the hook received.
//
// The SDK preserves the cancellation, the deadline, and the values of the
// context it gave the outermost hook. When a hook passes fn any context
// other than the one it received, the user code observes a context that is
// cancelled when either context is cancelled, reports the earlier of the
// two deadlines, and falls back to the SDK's context for values the hook's
// context lacks. This holds whether or not the hook's context descends from
// the SDK's. A hook therefore cannot detach the wrapped work from the
// invocation's cancellation or deadline. That merged context is cancelled
// once fn returns; user code must not keep using it after the step body,
// condition check, child context function, or handler it was given to has
// returned.
//
// The SDK runs the wrapped work at most once regardless of what the hook
// does. A hook that calls fn again receives the first call's result; the
// context passed to the later call is ignored. A hook that panics is
// contained: if it panics before calling fn, the SDK runs fn once with the
// ctx the hook received and uses that result; if it panics after calling
// fn, the SDK uses the result fn already produced. If fn itself panics,
// that panic reaches the SDK as it would without the hook, even if the hook
// recovers it and returns normally or calls fn again. A hook that panics
// while fn is still running on another goroutine fails the wrapped work
// with an error; fn is still not run again.
type Plugin struct {
	// OnInvocationStart is called once at the start of each Lambda
	// invocation, before the user handler runs.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	OnInvocationStart func(ctx context.Context, info InvocationHookInfo)

	// OnInvocationEnd is called once when the invocation ends, with the
	// invocation's status: Succeeded or Failed when the execution has
	// finished, Pending when the invocation suspended on an operation that
	// completes later, and Retrying when the invocation returned an error
	// to Lambda and the service will invoke the execution again. See the
	// [PluginInvocationStatus] constants.
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
	// (index 0) wraps first. fn must be called exactly once; the context
	// passed to fn is the parent of the handler's [Context]. See the
	// wrap-hook contract in the [Plugin] documentation.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	WrapInvocation func(ctx context.Context, info InvocationHookInfo, fn func(ctx context.Context) (any, error)) (any, error)

	// WrapOperationAttemptFn wraps the execution of an operation attempt
	// body (step fn, condition check). fn must be called exactly once; the
	// context passed to fn is the parent of the body's [StepContext]. See
	// the wrap-hook contract in the [Plugin] documentation.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	WrapOperationAttemptFn func(ctx context.Context, info AttemptHookInfo, fn func(ctx context.Context) (any, error)) (any, error)

	// WrapChildContextFn wraps the execution of a child-context function.
	// fn must be called exactly once; the context passed to fn is the
	// parent of the child [Context]. See the wrap-hook contract in the
	// [Plugin] documentation.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	WrapChildContextFn func(ctx context.Context, info OperationHookInfo, fn func(ctx context.Context) (any, error)) (any, error)

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
	// need both to avoid state ordering dependencies. It is a subset of
	// Operations.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	UpdatedOperations map[string]OperationHookInfo

	// Operations contains every operation known at the start of the
	// invocation, keyed by operation ID, including the root execution
	// operation and operations that did not change since the previous
	// invocation. UpdatedOperations is a subset of it. On the first
	// invocation it holds the execution operation alone. Each entry
	// carries the checkpointed status, timestamps, result, and error, with
	// IsReplay set, as an OnOperationEnd hook would report the operation.
	//
	// The map is built before the invocation hooks run and is not
	// modified afterwards, so a plugin may read it at any time. It is nil
	// when no registered plugin implements OnInvocationStart or
	// WrapInvocation, the hooks that receive it.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	Operations map[string]OperationHookInfo
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
// Succeeded and Failed are terminal: the execution has finished and no
// further invocation follows. Pending and Retrying both mean the execution
// continues in a later invocation; they differ in why this one ended.
//
// EXPERIMENTAL: these constants are experimental and may be changed or
// removed in future releases.
const (
	// PluginInvocationSucceeded reports that the handler returned a result
	// and the execution has succeeded. ExecutionResult carries the result.
	PluginInvocationSucceeded PluginInvocationStatus = "SUCCEEDED"

	// PluginInvocationFailed reports that the execution has failed: the
	// handler returned an error that is not scoped to the invocation, or
	// the SDK could not record the result and the failure is scoped to the
	// execution. ExecutionError carries the error. The execution is not
	// invoked again.
	PluginInvocationFailed PluginInvocationStatus = "FAILED"

	// PluginInvocationPending reports that the invocation suspended
	// normally: the handler is blocked on an operation that completes
	// later, such as a wait, a callback, or a chained invoke, or the
	// service stopped accepting this invocation's checkpoints. The
	// execution resumes in a later invocation once that operation
	// completes. ExecutionResult and ExecutionError are nil.
	PluginInvocationPending PluginInvocationStatus = "PENDING"

	// PluginInvocationRetrying reports that the invocation ended by
	// returning an error to Lambda instead of reporting an outcome for the
	// execution: the handler returned an error scoped to the invocation, or
	// the SDK could not serialize or record the handler's result and that
	// failure is not scoped to the execution. The service invokes the
	// execution again from its last checkpoint. ExecutionError carries the
	// error. A plugin that opens a span per execution should leave it open,
	// as for Pending.
	PluginInvocationRetrying PluginInvocationStatus = "RETRYING"
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
	// Every reported operation's parent is itself reported, so a consumer
	// can follow ParentID from any reported operation up to the root. Under
	// [WithPluginChildOperationsDepth] the operations nested inside an
	// operation at the configured depth are not reported; that operation
	// carries ChildrenOmitted so the consumer can tell the subtree was
	// truncated rather than absent.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	ParentID string

	// ChildrenOmitted reports that the operations nested inside this one
	// are omitted from plugin notifications: the operation lies at the
	// depth set by [WithPluginChildOperationsDepth], so no hook fires for
	// anything it contains, and no reported operation names it as
	// ParentID. False when no depth is set or the operation lies above it.
	// A consumer that finds no children for an operation with this field
	// set must treat the subtree as unreported, not as empty.
	//
	// EXPERIMENTAL: this field is experimental and may be changed or
	// removed in future releases.
	ChildrenOmitted bool

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

// WithPluginChildOperationsDepth bounds the depth in the operation tree of
// the operations reported to plugins. Operations deeper than depth are
// omitted from every operation-level notification: OnOperationStart,
// OnOperationEnd, OnOperationAttemptStart, OnOperationAttemptEnd, the
// WrapOperationAttemptFn and WrapChildContextFn wrap hooks, which then run
// the wrapped work directly, and the Operations and UpdatedOperations maps
// of the invocation hooks. Omission affects notifications only: an omitted
// operation runs, retries, and checkpoints exactly as a reported one.
//
// Depth counts the operations between an operation and the root of the
// tree. An operation claimed on the handler's [Context] has depth 0. An
// operation claimed inside a child context has the depth of that
// context's operation plus one. A [Map] or [Parallel] item has the depth
// of its batch plus one, and the operations inside the item one more. An
// item under [NestingFlat] has no operation of its own, so the operations
// inside it have the depth of the batch plus one; likewise a child context
// under [WithChildVirtual] adds no level, so the operations inside it have
// the depth of the operations claimed on its parent. depth is the deepest
// depth reported: 0 reports only the operations claimed on the handler's
// Context, 1 also reports their direct children, and so on.
//
// An operation at depth is reported with ChildrenOmitted set on its
// [OperationHookInfo], so a consumer can tell that the operations inside
// it were withheld rather than absent. Every reported operation's parent
// is also reported, so ParentID chains stay complete; see
// [OperationHookInfo.ParentID].
//
// The default reports every depth. depth must not be negative; [Wrap] and
// [Start] panic on a negative value.
//
// EXPERIMENTAL: this function is experimental and may be changed or
// removed in future releases.
func WithPluginChildOperationsDepth(depth int) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) {
		o.pluginChildDepth = depth
		o.pluginChildDepthSet = true
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

// operationHookInfo returns the OperationHookInfo shared by every
// operation lifecycle hook dispatched for one operation on c: the
// execution ARN, the parent context's wire ID, the operation's identity,
// whether the operation is replayed from a checkpoint, and whether the
// operations inside it are omitted by the plugin depth bound. The caller
// sets the timestamps and, where they apply, Attempt, Result, and Error
// before passing the info to dispatchOperationStart or
// dispatchOperationEnd.
func (c *execContext) operationHookInfo(id, name, opType, subType string, isReplay bool) OperationHookInfo {
	return OperationHookInfo{
		ExecutionArn:    c.executionArn,
		ID:              id,
		Name:            name,
		Type:            opType,
		SubType:         subType,
		IsReplay:        isReplay,
		ParentID:        c.parentWireID(),
		ChildrenOmitted: c.childrenOmittedAt(c.hookDepth),
	}
}

// dispatchOperationStart notifies every plugin's OnOperationStart hook that
// the operation info describes, claimed on ec, has begun, reporting status.
// Dispatch goes through dispatchNotification, so a panicking hook is
// recovered and never affects execution, and a nil dispatcher is a no-op.
// An operation beyond the plugin depth bound has a nil dispatcher; see
// hooksAt.
//
// Each operation dispatches at most one start per invocation. A live
// operation reports PluginOperationStarted. An operation replayed before it
// has settled reports its checkpointed status, so a plugin can tell a
// replayed start from a live one by info.IsReplay and by the status. Each
// operation decides whether a terminal replay also dispatches a start; see
// the operation's own documentation.
func dispatchOperationStart(ec *execContext, info OperationHookInfo, status PluginOperationStatus) {
	dispatchOperationStartTo(ec.operationHooks(), ec, info, status)
}

// dispatchOperationStartTo is dispatchOperationStart with the dispatcher
// chosen by the caller, for an operation whose depth differs from that of
// the operations claimed on ctx: a batch item dispatched from the batch's
// context. ctx is the context the hooks receive.
func dispatchOperationStartTo(d *pluginDispatcher, ctx context.Context, info OperationHookInfo, status PluginOperationStatus) {
	info.Status = status
	dispatchNotification(d, func(p *Plugin) {
		if p.OnOperationStart != nil {
			p.OnOperationStart(ctx, info)
		}
	})
}

// dispatchOperationEnd notifies every plugin's OnOperationEnd hook that the
// operation info describes, claimed on ec, has reached the terminal status.
// Dispatch goes through dispatchNotification, so a panicking hook is
// recovered and never affects execution, and a nil dispatcher is a no-op.
// An operation beyond the plugin depth bound has a nil dispatcher; see
// hooksAt.
//
// Each operation dispatches at most one end per invocation, and only once
// it has a terminal outcome. An operation that suspends the invocation has
// no outcome yet, so it dispatches no end; the invocation that observes
// its terminal checkpoint dispatches the end, with info.IsReplay set.
func dispatchOperationEnd(ec *execContext, info OperationHookInfo, status PluginOperationStatus) {
	dispatchOperationEndTo(ec.operationHooks(), ec, info, status)
}

// dispatchOperationEndTo is dispatchOperationEnd with the dispatcher chosen
// by the caller; see dispatchOperationStartTo.
func dispatchOperationEndTo(d *pluginDispatcher, ctx context.Context, info OperationHookInfo, status PluginOperationStatus) {
	info.Status = status
	dispatchNotification(d, func(p *Plugin) {
		if p.OnOperationEnd != nil {
			p.OnOperationEnd(ctx, info)
		}
	})
}

// wrapBody is the wrapped work a wrap hook receives: it runs the body with
// the context the hook supplies.
type wrapBody = func(context.Context) (any, error)

// wrapHook is one plugin's wrap hook with its info argument bound: it
// receives the context to pass on and the next body in the chain.
type wrapHook = func(context.Context, wrapBody) (any, error)

// wrapChain composes plugins' wrap hooks around fn. plugins[0] is outermost
// and receives ctx. Each hook passes a context to the next body; the
// innermost body runs fn with that context, re-attached to ctx by
// bodyContext so the body keeps ctx's cancellation, deadline, and values. A
// panicking wrapper is skipped without running fn a second time; see
// invokeWrapSafely. Returns fn(ctx) unchanged when d is nil.
//
// getWrap extracts the wrap function from a plugin; return nil if the
// plugin does not implement this particular wrap hook.
func wrapChain(d *pluginDispatcher, ctx context.Context, getWrap func(*Plugin) wrapHook, fn wrapBody) (any, error) {
	if d == nil {
		return fn(ctx)
	}

	// Build chain: plugins[0] outermost via reduceRight
	next := func(supplied context.Context) (any, error) {
		bodyCtx, release := bodyContext(ctx, supplied)
		defer release()
		return fn(bodyCtx)
	}
	for i := len(d.plugins) - 1; i >= 0; i-- {
		wrap := getWrap(&d.plugins[i])
		if wrap == nil {
			continue
		}
		innerNext := next
		wrapFn := wrap
		next = func(c context.Context) (any, error) {
			return invokeWrapSafely(c, wrapFn, innerNext)
		}
	}
	return next(ctx)
}

// bodyContext returns the context the wrapped body runs with, given the
// context the SDK gave the outermost hook (orig) and the one the innermost
// hook passed to fn (supplied). The second result releases what bodyContext
// allocated; the caller runs it once the body has returned.
//
// A nil supplied context, or orig itself, leaves the body with orig and
// returns a no-op release. Any other supplied context is merged with orig,
// whether or not it descends from orig: a context.Context is an interface,
// so nothing observable proves that a supplied context forwards orig's
// cancellation, deadline, and values. In particular, equal Done channels
// prove nothing. Two unrelated contexts that are never cancelled both
// report a nil Done channel, and a context may forward orig's Done while
// reporting a different Deadline or hiding orig's values. The body
// therefore gets a context that is cancelled when either orig or supplied
// is, that reports the earlier of the two deadlines, and that reads values
// from supplied first and from orig when supplied lacks them.
//
// The merged context is bound to the body. While the body runs, orig's
// cancellation reaches it through a callback registered on orig. When orig
// ends first, the merged context is cancelled with orig's cause. When the
// body returns first, release removes that callback from orig and cancels
// the merged context. One invocation may run many wrapped bodies, so
// without release each body would leave a callback and a context object
// attached to orig until the invocation itself ended.
func bodyContext(orig, supplied context.Context) (context.Context, func()) {
	if supplied == nil || sameContext(orig, supplied) {
		return orig, func() {}
	}

	merged := supplied
	releaseDeadline := func() {}
	if d, ok := orig.Deadline(); ok {
		if sd, sok := supplied.Deadline(); !sok || d.Before(sd) {
			merged, releaseDeadline = context.WithDeadline(merged, d)
		}
	}
	merged, cancel := context.WithCancelCause(merged)
	// Cancel the body's context when orig ends, with orig's cause, and
	// release the deadline context after it so the cause is not replaced
	// by a plain cancellation. A supplied context that ends first cancels
	// merged on its own, through the parent chain.
	stop := context.AfterFunc(orig, func() {
		cancel(context.Cause(orig))
		releaseDeadline()
	})
	release := func() {
		stop()
		cancel(nil)
		releaseDeadline()
	}
	return &fallbackValueContext{Context: merged, fallback: orig}, release
}

// sameContext reports whether a and b are the same context value.
//
// Comparing two interface values panics when both hold the same dynamic
// type and that type is not comparable, for example a struct with a slice
// field. A context.Context implementation may be such a type, and a hook
// that passes its context through unchanged then hands bodyContext two
// copies of it. So the comparison is guarded: two values of the same
// non-comparable type are reported as different, and bodyContext merges
// them, which keeps the body's values, deadline, and cancellation intact.
func sameContext(a, b context.Context) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Type() != vb.Type() {
		return false
	}
	if !va.Comparable() {
		return false
	}
	return a == b
}

// fallbackValueContext is a context that reads a value from Context first
// and from fallback when Context lacks it. Done, Err, and Deadline come from
// Context alone. Because Done and the cancellation lookup the standard
// library performs through Value both resolve to Context, contexts derived
// from a fallbackValueContext propagate cancellation the same way as ones
// derived from Context directly.
type fallbackValueContext struct {
	context.Context
	fallback context.Context
}

func (c *fallbackValueContext) Value(key any) any {
	if v := c.Context.Value(key); v != nil {
		return v
	}
	return c.fallback.Value(key)
}

// errWrapBodyUnfinished is the error returned when a wrap hook panics while
// the wrapped body it started on another goroutine has not yet returned.
// The body is not run again, and its result is not available.
var errWrapBodyUnfinished = errors.New("durable: plugin wrap hook panicked before the wrapped function returned; result unavailable")

// guardedFn runs a wrapped body at most once, whatever the wrap hooks
// around it do. The first call runs fn with the context that call supplies
// and records its outcome. Every later call returns the recorded outcome
// without running fn again, whatever context it supplies. A panic in fn is
// recorded and re-raised on the first call and on every later call, so a
// hook that recovers the body's panic cannot turn it into a normal result
// by calling fn again.
type guardedFn struct {
	fn wrapBody

	// hookCtx is the context the hook around this body received. It
	// stands in for a nil context supplied to call, and it is the context
	// fn runs with when the hook panics before calling fn.
	hookCtx context.Context

	mu       sync.Mutex
	entered  bool // fn has been called
	done     bool // fn returned or panicked
	result   any
	err      error
	panicked bool
	panicVal any
}

// call runs fn(ctx) on the first call and returns the recorded outcome on
// every later call. A nil ctx stands for the hook's own context. A later
// call after fn panicked re-raises that panic. A later call while fn is
// still running on another goroutine returns errWrapBodyUnfinished.
func (g *guardedFn) call(ctx context.Context) (any, error) {
	if ctx == nil {
		ctx = g.hookCtx
	}
	g.mu.Lock()
	if g.entered {
		result, err, done, panicked, panicVal := g.result, g.err, g.done, g.panicked, g.panicVal
		g.mu.Unlock()
		if !done {
			return nil, errWrapBodyUnfinished
		}
		if panicked {
			panic(panicVal)
		}
		return result, err
	}
	g.entered = true
	g.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			g.mu.Lock()
			g.done = true
			g.panicked = true
			g.panicVal = r
			g.mu.Unlock()
			panic(r)
		}
	}()
	result, err := g.fn(ctx)
	g.mu.Lock()
	g.result, g.err, g.done = result, err, true
	g.mu.Unlock()
	return result, err
}

// rethrowBodyPanic re-raises the recorded panic when fn panicked. It is a
// no-op otherwise. Called after a wrap hook returns normally, so a hook
// that recovered the body's panic cannot report the body as successful.
func (g *guardedFn) rethrowBodyPanic() {
	g.mu.Lock()
	panicked, panicVal := g.panicked, g.panicVal
	g.mu.Unlock()
	if panicked {
		panic(panicVal)
	}
}

// invokeWrapSafely calls wrapFn(ctx, innerFn), recovering panics raised by
// the hook. innerFn is guarded so it runs at most once, with the context
// the hook passes it. If the hook panics before calling innerFn, innerFn
// runs once with ctx. If the hook panics after calling innerFn, the
// recorded result of that call is returned. If innerFn itself panicked,
// that panic reaches the caller even when the hook recovers it, calls
// innerFn again, or returns normally. A hook that calls innerFn more than
// once receives the first call's outcome on every call after it.
func invokeWrapSafely(ctx context.Context, wrapFn wrapHook, innerFn wrapBody) (result any, err error) {
	g := &guardedFn{fn: innerFn, hookCtx: ctx}
	defer func() {
		if r := recover(); r != nil {
			// Either the hook panicked or the body's panic propagated
			// through the hook. call runs the body when it never ran,
			// returns its recorded outcome when it did, and re-raises
			// the body's own panic when it panicked.
			result, err = g.call(ctx)
		}
	}()
	result, err = wrapFn(ctx, g.call)
	g.rethrowBodyPanic()
	return result, err
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

// hasInvocationInfoConsumer reports whether at least one registered plugin
// implements a hook that receives an InvocationHookInfo: OnInvocationStart
// or WrapInvocation. False for a nil dispatcher. The handler builds
// InvocationHookInfo.Operations only when this is true.
func (d *pluginDispatcher) hasInvocationInfoConsumer() bool {
	if d == nil {
		return false
	}
	for i := range d.plugins {
		if d.plugins[i].OnInvocationStart != nil || d.plugins[i].WrapInvocation != nil {
			return true
		}
	}
	return false
}

// checkpointedOperationInfo returns the OperationHookInfo that describes the
// checkpointed operation op of the execution executionArn, as the invocation
// hooks report it: the wire ID, the checkpointed status, timestamps, result,
// and error, with IsReplay set because the operation was recorded before
// this invocation. ChildrenOmitted is left false; the caller sets it from
// the operation's depth when a plugin depth bound is in effect.
func checkpointedOperationInfo(executionArn string, op *operation) OperationHookInfo {
	return OperationHookInfo{
		ExecutionArn:   executionArn,
		ID:             op.id,
		Name:           op.name,
		Type:           op.opType,
		SubType:        op.subType,
		Status:         toPluginOperationStatus(op.status),
		IsReplay:       true,
		ParentID:       op.parentID,
		StartTimestamp: op.startTimestamp,
		EndTimestamp:   op.endTimestamp,
		Result:         op.operationResult(),
		Error:          op.operationError(),
	}
}

// checkpointedOperationDepth returns the depth in the operation tree of the
// checkpointed operation op, capped at limit: the number of operations
// between op and the root, found by following the wire parent IDs through
// state, or limit when the chain is at least that long. An operation with
// no parent, including the execution operation itself and every top-level
// operation, has depth 0. A parent ID the state does not hold still counts
// as one level; the walk stops there.
//
// The caller only needs to know whether the depth reaches limit, and the
// exact depth below it. Stopping at limit keeps the walk at most limit
// steps per operation, so a chain of n nested operations costs O(n*limit)
// rather than O(n^2), and a cyclic parent chain from malformed execution
// state terminates. limit must be positive.
func checkpointedOperationDepth(state *executionState, op *operation, limit int) int {
	depth := 0
	for parentID := op.parentID; parentID != "" && depth < limit; {
		depth++
		parent := state.getByWireID(parentID)
		if parent == nil {
			break
		}
		parentID = parent.parentID
	}
	return depth
}

// addCheckpointedOperationInfo adds the OperationHookInfo of the
// checkpointed operation op to m, keyed by wire ID, when the plugin depth
// bound reports it, with ChildrenOmitted set when op lies at the bound.
// bound is the root context's hookDepthBound; 0 reports every operation
// and skips the depth walk. Otherwise the walk stops at bound, the first
// depth the bound omits.
func addCheckpointedOperationInfo(m map[string]OperationHookInfo, executionArn string, state *executionState, bound int, op *operation) {
	info := checkpointedOperationInfo(executionArn, op)
	if bound != 0 {
		depth := checkpointedOperationDepth(state, op, bound)
		if depth >= bound {
			return
		}
		info.ChildrenOmitted = depth == bound-1
	}
	m[op.id] = info
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
