package durable

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// executionMode is the context's position in the checkpoint-replay
// lifecycle.
type executionMode int

const (
	// modeExecution runs operations live and checkpoints their results.
	modeExecution executionMode = iota + 1

	// modeReplay returns checkpointed results without re-executing, until
	// the first operation with no checkpoint is reached.
	modeReplay

	// modeReplaySucceededContext replays inside a child context whose
	// overall result is already checkpointed. Operations that were still
	// in flight when the context succeeded must not re-execute: unlike
	// modeReplay, an operation with no terminal checkpoint does not flip
	// the context to live execution. A synchronous await of such an
	// operation parks with a bounded deadline (see parkUnfinishedReplay).
	modeReplaySucceededContext
)

// execContext is the concrete [Context] implementation.
type execContext struct {
	context.Context

	executionArn string
	invocation   invocationInfo

	// logCfg holds the logging settings in effect on this context: the
	// invocation's handler with the execution attributes (request ID,
	// execution ARN, tenant ID) already attached, and the replay log mode.
	// The handler carries no operation attributes, so every operation
	// scope (a child context, a step body) adds its own to it and never
	// inherits another scope's. That keeps operationId and operationName
	// single-valued in a record. Every read goes through logDefaults and
	// every write through setLogDefaults, so a ConfigureLogging call on
	// the owning goroutine never races with a read from another
	// goroutine. It is always non-nil once the context is constructed.
	//
	// logScope is this context's own operation attributes: empty for the
	// root context, the child operation's ID and name for a child context,
	// and the parent's scope for a branch. ctxLogger is the handler plus
	// logScope, wrapped with replay suppression driven by this context's
	// own mode, so suppression is decided per branch. It is rebuilt by
	// attachLogger when the handler changes; Logger loads it atomically.
	logCfg    atomic.Pointer[logDefaults]
	logScope  []slog.Attr
	ctxLogger atomic.Pointer[slog.Logger]

	// mode tracks the execution's replay lifecycle position. Accessed
	// atomically because child goroutines read it concurrently with the
	// owning goroutine's refreshReplayMode write.
	mode  atomic.Int32
	ids   *opIDs
	owner goroutineOwner
	state *executionState

	// checkpointParent is the operation ID that operations claimed on this
	// context record as their ParentId. It equals ids.prefix for the root
	// context (empty) and for a child context whose own operation is
	// checkpointed. A virtual child context has no checkpoint of its own,
	// so its operations record the nearest checkpointed ancestor instead;
	// see virtualChild.
	checkpointParent string

	// virtual reports whether this context is a virtual child context made
	// by [WithChildVirtual]: one with an operation-ID namespace of its own
	// but no checkpoint of its own. A virtual child context cannot hold
	// another; see [WithChildVirtual]. A batch item in [NestingFlat] mode
	// is also virtual in the checkpoint sense but does not set this flag,
	// because a virtual child inside a flat item is supported.
	virtual bool

	// blocked is set when an operation on this context enters a pending
	// state (commits to suspension). Once set, subsequent claims on this
	// same context fail with errSuspendExecution, preventing user code
	// that swallows errors from starting new operations. Sibling contexts
	// (different goroutines) are not affected.
	blocked atomic.Bool

	// abandon, when non-nil, is the handle of the batch branch subtree this
	// context runs under. A claim on a context whose subtree, or any
	// enclosing subtree, has been abandoned returns errSuspendExecution so
	// the branch stops starting new work and unwinds. It is shared by a
	// batch branch's whole child-context subtree; nil for every other
	// context.
	abandon *abandonHandle

	// branchTok is the active-branch token for the goroutine that owns this
	// context. A pending callback's pre-result hook releases it so the
	// calling branch is deregistered exactly once. It is shared by a
	// context's child and branch contexts, mirroring abandon; nil until a
	// branch is registered.
	branchTok *branchToken

	// ownsBranchTok reports whether this context is the one that
	// registered branchTok, through adoptBranchToken. A context derived by
	// child or branch inherits the token with ownsBranchTok false. The
	// park path (parkUnfinishedReplay) releases the token only when the
	// parking context owns it: a child that parks on the root goroutine
	// must not deregister the root handler's branch, because the root
	// goroutine is not finished; it will observe the park outcome and
	// decide the invocation's response itself.
	ownsBranchTok bool

	// serdesCfg holds the handler-level serializer defaults in effect on
	// this context. It is always non-nil once the context is constructed.
	// Every read goes through serdesDefaults and every write through
	// setSerdesDefaults, so a ConfigureSerdes call on the owning goroutine
	// never races with a read from another goroutine; a reader always sees
	// either the previous snapshot or the new one, never a torn value.
	serdesCfg atomic.Pointer[serdesDefaults]

	// suspend is the invocation-wide suspension signal, shared across
	// the root and all child contexts.
	suspend *suspendSignal

	// checkpointer persists operation updates; shared across the root and
	// all child contexts of one invocation.
	checkpointer *checkpointer

	// pluginDispatcher dispatches lifecycle hooks to registered plugins.
	// Nil when no plugins are registered (zero overhead). Operation-level
	// hooks go through operationHooks, which also applies the depth bound
	// below; invocation-level hooks and log enrichment use it directly.
	pluginDispatcher *pluginDispatcher

	// hookDepth is the depth in the operation tree of the operations
	// claimed on this context: 0 on the root context, and one more than
	// the depth of the parent operation for every other context. See
	// [WithPluginChildOperationsDepth] for how depth is counted.
	hookDepth int

	// hookDepthBound is one more than the deepest depth whose operations
	// are reported to plugins, or 0 when every depth is reported. Set on
	// the root context from [WithPluginChildOperationsDepth] and shared by
	// every context derived from it. Stored as depth+1 so that the zero
	// value keeps the default behavior.
	hookDepthBound int

	// executionStartTime is the checkpointed start timestamp of the root
	// EXECUTION operation. It is the same value on every invocation of
	// one execution, making it safe to use between durable operations.
	executionStartTime time.Time

	// noStackTraces disables stack trace capture for failures produced by
	// user code. Set from [WithStackTraces]; the zero value keeps capture
	// enabled. Shared by the root and all child contexts.
	noStackTraces bool

	// combinatorObserve, when non-nil, is called by the [Any] and [Race]
	// receive loops after each future outcome is observed. It is set only
	// by tests, on one context before its combinators run, so that a
	// sibling future can be released after a specific outcome has been
	// seen. Child and branch contexts inherit it at creation, so a value
	// set on a context before it derives children reaches every
	// combinator under it without any later write. Nil in production.
	combinatorObserve func(err error)
}

var _ Context = (*execContext)(nil)

func (c *execContext) sealed() {}

// newExecContext creates the root context for one invocation. The mode
// starts in replay when checkpointed operations beyond the always-present
// execution operation exist.
func newExecContext(ctx context.Context, executionArn string, inv invocationInfo, handler slog.Handler, state *executionState) *execContext {
	mode := modeExecution
	if state.numOperations() > 1 {
		mode = modeReplay
	}
	ec := &execContext{
		Context:      ctx,
		executionArn: executionArn,
		invocation:   inv,
		ids:          &opIDs{},
		owner:        currentGoroutineOwner(),
		state:        state,
		suspend:      newSuspendSignal(),
	}
	ec.setSerdesDefaults(serdesDefaults{serdes: JSONSerdes})
	ec.setLogDefaults(logDefaults{handler: handler.WithAttrs(executionLogAttrs(executionArn, inv))})
	ec.mode.Store(int32(mode))
	ec.attachLogger()
	return ec
}

// attachLogger builds this context's replay-aware logger: the execution
// handler in effect with this context's own operation scope added. The
// wrapper reads this context's mode and replay log mode on every call, so
// a still-replaying branch stays suppressed regardless of what sibling
// contexts are doing, and a mode change reaches loggers already handed
// out. Every constructor (newExecContext, child, branch) calls
// attachLogger after storing the initial mode, logScope, and log
// defaults; configureLogging calls it again after replacing the handler.
func (c *execContext) attachLogger() {
	c.ctxLogger.Store(newReplayLogger(c.scopedLogHandler(c.logScope), c.IsReplaying, c.emitReplayedLogs))
}

// scopedLogHandler returns the execution handler in effect with attrs
// added, or the handler itself when attrs is empty. The handler is the
// execution-scoped one, never a context's already-scoped handler, so a
// nested operation's attributes do not repeat an enclosing scope's keys.
func (c *execContext) scopedLogHandler(attrs []slog.Attr) slog.Handler {
	h := c.logDefaults().handler
	if len(attrs) == 0 {
		return h
	}
	return h.WithAttrs(attrs)
}

// operationLogger returns the logger handed to a step body, condition
// check, or callback submitter of the operation with positional ID id: the
// execution handler with the operation's own attributes added, in place of
// this context's scope, under this context's replay suppression.
func (c *execContext) operationLogger(id, name string, attempt int) *slog.Logger {
	return newReplayLogger(c.scopedLogHandler(operationLogAttrs(id, name, attempt)), c.IsReplaying, c.emitReplayedLogs)
}

// emitReplayedLogs reports whether records this context emits while
// replaying are emitted rather than dropped; see [ReplayLogModeEmit]. It
// is the emitReplayed func of every replayHandler built for this context.
func (c *execContext) emitReplayedLogs() bool {
	return c.logDefaults().emitReplayed
}

// logDefaults is the pair of logging settings in effect on one context. A
// context stores it behind an atomic pointer and never mutates a stored
// value: configureLogging builds a new value and swaps the pointer, so a
// copy taken before the call still describes the settings from before it.
type logDefaults struct {
	// handler is the installed handler, wrapped with plugin enrichment
	// when a plugin implements EnrichLogContext, with the execution
	// attributes attached. Operation scopes add their own attributes to
	// it; see scopedLogHandler.
	handler slog.Handler

	// emitReplayed is true under [ReplayLogModeEmit]: records emitted
	// while replaying are passed on with the replay attribute instead of
	// being dropped.
	emitReplayed bool
}

// logDefaults reads c's current logging settings. It is safe to call from
// any goroutine: the load is atomic, so a concurrent configureLogging on
// the owner yields either the old snapshot or the new one.
func (c *execContext) logDefaults() logDefaults {
	return *c.logCfg.Load()
}

// setLogDefaults installs l as c's logging settings. The pointer store is
// atomic, so readers on other goroutines never observe a torn pair; every
// caller other than the constructors runs on c's owning goroutine.
func (c *execContext) setLogDefaults(l logDefaults) {
	c.logCfg.Store(&l)
}

// configureLogging implements [ConfigureLogging]. The owner check keeps the
// documented rule that logging settings change only from the goroutine
// that owns the context, the same guard claimOperation applies to
// operations. A new handler is wrapped exactly as the construction-time
// handler was: plugin enrichment first, then the execution attributes;
// the context's logger is then rebuilt so Logger returns one over the new
// handler. The replay log mode is stored only; every replayHandler of
// this context reads it through emitReplayedLogs on each record.
//
// The owner may keep running after it launches an asynchronous operation,
// and may then call configureLogging while that operation's goroutine is
// still starting. So an asynchronous operation takes an inheritedDefaults
// snapshot on the owning goroutine before the go statement and derives its
// context through childWith or branchWith. That keeps the documented rule:
// a context derived before the call keeps the settings it was derived with.
func (c *execContext) configureLogging(cfg LogConfig) error {
	if err := c.owner.check(); err != nil {
		return fmt.Errorf("durable: ConfigureLogging: %w", err)
	}
	l := c.logDefaults()
	switch cfg.ReplayLogMode {
	case ReplayLogModeSuppress:
		l.emitReplayed = false
	case ReplayLogModeEmit:
		l.emitReplayed = true
	case ReplayLogModeUnchanged:
	}
	if cfg.Handler != nil {
		h := newEnrichLogHandler(cfg.Handler, c.pluginDispatcher)
		l.handler = h.WithAttrs(executionLogAttrs(c.executionArn, c.invocation))
	}
	c.setLogDefaults(l)
	if cfg.Handler != nil {
		c.attachLogger()
	}
	return nil
}

// inheritedDefaults is the configuration a derived context copies from its
// parent at derivation: the serializer defaults and the logging settings.
// An asynchronous operation snapshots it on the owning goroutine before
// the go statement; see configureSerdes and configureLogging.
type inheritedDefaults struct {
	serdes serdesDefaults
	log    logDefaults
}

// inheritedDefaults reads c's current serializer defaults and logging
// settings as one snapshot.
func (c *execContext) inheritedDefaults() inheritedDefaults {
	return inheritedDefaults{serdes: c.serdesDefaults(), log: c.logDefaults()}
}

func (c *execContext) ExecutionArn() string { return c.executionArn }

// serdesCtx builds a [SerdesContext] for the given operation ID, using the
// execution's ARN as the durable execution identifier.
func (c *execContext) serdesCtx(operationID string) SerdesContext {
	return SerdesContext{
		OperationID:         operationID,
		DurableExecutionArn: c.executionArn,
	}
}

// serdesDefaults is the pair of handler-level serializer defaults in effect
// on one context. A context stores it behind an atomic pointer and never
// mutates a stored value: ConfigureSerdes builds a new value and swaps the
// pointer. So a value read from a context is fixed, and a copy taken before
// a ConfigureSerdes call still describes the defaults from before the call.
type serdesDefaults struct {
	// serdes is the handler-level default serializer for operation
	// results. Per-operation serdes options take precedence.
	serdes Serdes

	// callbackDeserializer is the handler-level default deserializer for
	// callback payloads submitted by external systems. When nil, callbacks
	// use the standard serdes.
	callbackDeserializer Deserializer
}

// serdesDefaults reads c's current serializer defaults. It is safe to call
// from any goroutine: the load is atomic, so a concurrent ConfigureSerdes
// on the owner yields either the old snapshot or the new one. An operation
// that reads the defaults before claimOperation rejects it with
// ErrWrongGoroutine therefore reads a consistent value and then fails
// cleanly.
func (c *execContext) serdesDefaults() serdesDefaults {
	return *c.serdesCfg.Load()
}

// setSerdesDefaults installs d as c's serializer defaults. The pointer
// store is atomic, so readers on other goroutines never observe a torn
// pair; every caller other than the constructors runs on c's owning
// goroutine.
func (c *execContext) setSerdesDefaults(d serdesDefaults) {
	c.serdesCfg.Store(&d)
}

// configureSerdes implements [ConfigureSerdes]. The owner check keeps the
// documented rule that serializer defaults change only from the goroutine
// that owns the context, the same guard claimOperation applies to
// operations. The atomic swap in setSerdesDefaults makes the change itself
// safe against operations that read the defaults from another goroutine
// before their own owner check rejects them.
//
// The owner may keep running after it launches an asynchronous operation,
// and may then call configureSerdes while that operation's goroutine is
// still starting. So an asynchronous operation takes a serdesDefaults
// snapshot on the owning goroutine before the go statement and derives its
// context through childWith or branchWith. That keeps the documented rule:
// a context derived before the call keeps the defaults it was derived with.
func (c *execContext) configureSerdes(cfg SerdesConfig) error {
	if err := c.owner.check(); err != nil {
		return fmt.Errorf("durable: ConfigureSerdes: %w", err)
	}
	d := c.serdesDefaults()
	if cfg.Serdes != nil {
		d.serdes = cfg.Serdes
	}
	if cfg.CallbackDeserializer != nil {
		d.callbackDeserializer = cfg.CallbackDeserializer
	}
	c.setSerdesDefaults(d)
	return nil
}

func (c *execContext) RequestID() string { return c.invocation.requestID }

func (c *execContext) InvokedFunctionARN() string { return c.invocation.invokedFunctionARN }

func (c *execContext) Logger() *slog.Logger { return c.ctxLogger.Load() }

func (c *execContext) IsReplaying() bool {
	m := executionMode(c.mode.Load())
	return m == modeReplay || m == modeReplaySucceededContext
}

// parentWireID returns the hashed wire-format parent context ID for use in
// OperationHookInfo.ParentID. Returns empty string for root-level operations.
func (c *execContext) parentWireID() string {
	if c.checkpointParent == "" {
		return ""
	}
	return hashID(c.checkpointParent)
}

// parentOperationID returns the operation ID that operations claimed on
// this context record as their ParentId, or the empty string at the root.
// Checkpoint update builders hash this value into the ParentId field.
func (c *execContext) parentOperationID() string {
	return c.checkpointParent
}

// claimOperation validates goroutine ownership, refreshes the replay mode
// for the pending operation, and claims the next operation ID. Every
// durable operation begins with a claimOperation call on its context.
//
// If the invocation is already suspending, claimOperation fails with
// errSuspendExecution so the operation unwinds without checkpointing.
//
// On error, no state is mutated: the operation ID is not consumed and the
// mode is unchanged.
func (c *execContext) claimOperation() (string, error) {
	if err := c.claimable(); err != nil {
		return "", err
	}
	c.refreshReplayMode()
	return c.ids.next(), nil
}

// claimUncheckpointedOperation claims the next operation ID for an
// operation that records no checkpoint of its own: a virtual child context
// ([WithChildVirtual]). It applies the same checks as claimOperation but
// leaves the replay mode as it is. The claimed ID is never in the
// checkpoint log, so its absence says nothing about where replay ends; the
// next checkpointed operation claimed on this context, or inside the
// virtual child, settles that.
func (c *execContext) claimUncheckpointedOperation() (string, error) {
	if err := c.claimable(); err != nil {
		return "", err
	}
	return c.ids.next(), nil
}

// claimable reports whether this context may claim an operation: the
// invocation is not suspending, the context is not blocked or abandoned,
// and the calling goroutine owns it.
func (c *execContext) claimable() error {
	if c.suspend.fired() {
		return errSuspendExecution
	}
	if c.blocked.Load() {
		return errSuspendExecution
	}
	if c.abandon.abandoned() {
		return errSuspendExecution
	}
	return c.owner.check()
}

// refreshReplayMode switches from replay to live execution when the
// pending operation has no checkpoint.
//
// A virtual child context is not checkpointed itself, so the pending ID can
// be absent from the state even though replay should continue: its first
// child operation IS checkpointed under "<pendingID>-1". Probe that child
// before concluding that replay has finished.
func (c *execContext) refreshReplayMode() {
	if executionMode(c.mode.Load()) != modeReplay {
		return
	}
	pendingID := c.ids.peek()
	if c.state.get(pendingID) != nil {
		return
	}
	if c.state.get(pendingID+"-1") != nil {
		return
	}
	c.mode.Store(int32(modeExecution))
}

// unfinishedInSucceededContext reports whether the checkpointed operation
// must not run because this context replays inside a child context whose
// overall result is already recorded (modeReplaySucceededContext) while the
// operation itself has no terminal checkpoint. Such an operation was still
// in flight when the context's result was recorded: re-executing it would
// repeat its side effects, and it cannot produce a result in this
// invocation.
func (c *execContext) unfinishedInSucceededContext(op *operation) bool {
	if executionMode(c.mode.Load()) != modeReplaySucceededContext {
		return false
	}
	return op == nil || !op.status.terminal()
}

// unfinishedReplayParkTimeout bounds how long parkUnfinishedReplay waits
// for the invocation to suspend before it gives up and reports the
// unfinished operation as a determinism violation.
const unfinishedReplayParkTimeout = time.Second

// parkUnfinishedReplay handles a synchronous await of an unfinished
// operation inside an already-succeeded child context. op is the
// operation's checkpoint, or nil when it has none; id, opType, subType and
// name describe the operation the current code asked for.
//
// The operation must not execute and cannot settle in this invocation. No
// pending commitment is made: an unfinished operation inside a succeeded
// context must not force the invocation to PENDING. The caller therefore
// blocks until one of three events, then unwinds:
//
//  1. The invocation suspends because other branches committed to PENDING
//     and deregistered. The context is marked blocked and the caller
//     unwinds with errSuspendExecution.
//  2. unfinishedReplayParkTimeout elapses.
//  3. The Lambda context ends.
//
// Events 2 and 3 share one outcome, decided by whether a pending commitment
// exists at that moment. If one does, the invocation responds PENDING
// whatever the handler returns, so the context is marked blocked and the
// caller unwinds with errSuspendExecution. If none does, the caller unwinds
// with a *NonDeterministicReplayError that names the unfinished operation.
//
// Event 2 is what bounds the wait. A parking branch that is the last active
// branch has no pending commitment, so nothing can fire the suspend signal;
// without the deadline it would wait for the Lambda deadline, the
// invocation would end PENDING, and the next invocation would repeat the
// same wait. A synchronous await of an operation that had not completed
// when its context's result was recorded cannot occur in a deterministic
// handler: the live run must have returned from the context without
// awaiting it. So the deadline turns a silent stall into an error that
// names the operation, and the invocation returns within
// unfinishedReplayParkTimeout of reaching the park.
//
// Event 3 gets the same commitment check so that a Lambda context with less
// than unfinishedReplayParkTimeout remaining does not defeat the bound. If
// the context ended and the park unwound as a suspension regardless, the
// invocation would respond PENDING with no diagnostic and the next
// invocation would park again. Returning the determinism error instead
// fails the execution with the same diagnostic the deadline produces.
//
// The branch token is released before parking only when this context
// registered it (ownsBranchTok). An asynchronous operation's branch owns
// its token, so releasing it lets a sibling's commitment fire the signal
// while this goroutine is parked. A child context created on its parent's
// goroutine inherits the parent's token and does not own it; releasing
// that token would deregister a goroutine that is still running, and for
// the root handler it would let a commitment made after the handler
// returned change the invocation's outcome. Such a child parks with the
// token held, and the owning goroutine releases it when it unwinds.
func (c *execContext) parkUnfinishedReplay(op *operation, id, opType, subType, name string) error {
	if c.ownsBranchTok {
		c.branchTok.release()
	}
	timer := time.NewTimer(unfinishedReplayParkTimeout)
	defer timer.Stop()
	select {
	case <-c.suspend.done():
		c.blocked.Store(true)
		return errSuspendExecution
	case <-c.Done():
	case <-timer.C:
	}
	if c.suspend.committed() {
		c.blocked.Store(true)
		return errSuspendExecution
	}
	return newUnfinishedReplayError(op, id, opType, subType, name)
}

// newUnfinishedReplayError builds the error parkUnfinishedReplay returns
// when its deadline elapses or the Lambda context ends with no pending
// commitment. The message states which operation was awaited
// and why it can never settle, so a determinism violation is diagnosable
// from the execution's failure record.
func newUnfinishedReplayError(op *operation, id, opType, subType, name string) *NonDeterministicReplayError {
	e := &NonDeterministicReplayError{
		Name:            name,
		StepID:          id,
		ExpectedType:    opType,
		ExpectedSubType: subType,
		ExpectedName:    name,
	}
	checkpoint := "has no checkpoint"
	if op != nil {
		e.ActualType = op.opType
		e.ActualSubType = op.subType
		e.ActualName = op.name
		checkpoint = fmt.Sprintf("is checkpointed as %s", op.status)
	}
	e.detail = fmt.Sprintf(
		"durable: non-deterministic replay at step %q (name %q): "+
			"%s/%s was awaited inside a child context whose result is already recorded, "+
			"but the operation %s and had not completed when that result was recorded; "+
			"it does not run again during replay, so the await can never settle",
		id, name, opType, subType, checkpoint,
	)
	return e
}

// adoptBranchToken records tok as the active-branch registration made for
// this context's goroutine and marks this context as its owner. Contexts
// derived from this one by child or branch inherit the token without
// ownership, so only this context releases it on the park path.
func (c *execContext) adoptBranchToken(tok *branchToken) {
	c.branchTok = tok
	c.ownsBranchTok = true
}

// child creates the context for a child operation with the given entity ID
// and name. The child's log records carry the operation's hashed ID as
// operationId and, when name is non-empty, name as operationName.
//
// The caller is responsible for computing mode: a child whose own operation
// is already checkpointed as SUCCEEDED must receive
// modeReplaySucceededContext, not the parent's current mode. Owner is the
// goroutine that runs the child function, captured by the caller after that
// goroutine starts.
//
// The child inherits the parent's branch token without owning it. A caller
// that runs the child on a freshly registered goroutine replaces the token
// through adoptBranchToken.
//
// child copies c's serializer defaults and logging settings as they are at
// the call. A goroutine launched while the owner keeps running uses
// childWith with a snapshot taken before the go statement, so that a
// ConfigureSerdes or ConfigureLogging call between the go statement and
// the child's construction does not reach the child; see configureSerdes
// and configureLogging.
func (c *execContext) child(entityID, name string, owner goroutineOwner, mode executionMode) *execContext {
	return c.childWith(entityID, name, owner, mode, c.inheritedDefaults())
}

// childWith is child with the inherited defaults supplied by the caller
// instead of read from c.
func (c *execContext) childWith(entityID, name string, owner goroutineOwner, mode executionMode, d inheritedDefaults) *execContext {
	child := &execContext{
		Context:            c.Context,
		executionArn:       c.executionArn,
		invocation:         c.invocation,
		logScope:           contextLogAttrs(entityID, name),
		ids:                c.ids.child(entityID),
		owner:              owner,
		state:              c.state,
		checkpointParent:   entityID,
		suspend:            c.suspend,
		checkpointer:       c.checkpointer,
		pluginDispatcher:   c.pluginDispatcher,
		hookDepth:          c.hookDepth + 1,
		hookDepthBound:     c.hookDepthBound,
		executionStartTime: c.executionStartTime,
		noStackTraces:      c.noStackTraces,
		abandon:            c.abandon,
		branchTok:          c.branchTok,
		combinatorObserve:  c.combinatorObserve,
	}
	child.setSerdesDefaults(d.serdes)
	child.setLogDefaults(d.log)
	child.mode.Store(int32(mode))
	child.attachLogger()
	return child
}

// virtualChild creates the context for a virtual child operation: one that
// mints its own operation-ID namespace under entityID but is never
// checkpointed itself. Because no operation with ID entityID exists in the
// checkpoint log, operations claimed on the virtual child record
// parentID, the nearest checkpointed ancestor, as their ParentId. The
// caller computes mode as for child. name is the item's name for the log
// scope, as for child.
//
// The virtual child adds no level to the operation tree: its operations
// are children of the batch parentID, so their depth is the depth of the
// batch's items, whatever context c is; see batchItemDepth.
func (c *execContext) virtualChild(entityID, name, parentID string, owner goroutineOwner, mode executionMode) *execContext {
	vc := c.child(entityID, name, owner, mode)
	vc.checkpointParent = parentID
	vc.hookDepth = c.batchItemDepth(parentID)
	return vc
}

// virtualChildContextWith creates the context for a standalone virtual
// child context, the one [WithChildVirtual] selects, with the inherited
// defaults d supplied by the caller as for childWith. The child mints its
// operation IDs under entityID but is never checkpointed itself, so its
// operations record c's own checkpoint parent as their ParentId: they are
// recorded exactly where operations claimed on c are recorded. For the
// same reason they have the depth of c's operations, not one more. The
// caller computes mode with virtualChildReplayMode.
func (c *execContext) virtualChildContextWith(entityID, name string, owner goroutineOwner, mode executionMode, d inheritedDefaults) *execContext {
	vc := c.childWith(entityID, name, owner, mode, d)
	vc.checkpointParent = c.checkpointParent
	vc.hookDepth = c.hookDepth
	vc.virtual = true
	return vc
}

// batchItemChild creates the context for the checkpointed item childID of
// the batch parentID, as child does, with the depth of the item's
// operations set from the batch rather than from c. The item's own depth
// is the depth of the batch plus one; the operations inside it are one
// deeper. c is either the context that claimed the batch or a context
// minted for the batch itself; see batchItemDepth.
func (c *execContext) batchItemChild(childID, name, parentID string, owner goroutineOwner, mode executionMode) *execContext {
	child := c.child(childID, name, owner, mode)
	child.hookDepth = c.batchItemDepth(parentID) + 1
	return child
}

// batchItemDepth returns the depth in the operation tree of the items of
// the batch parentID when the batch's item code runs on c. Two contexts
// run item code. The context that claimed the batch mints the batch's ID
// from its own ID namespace, so the batch has the depth of c's operations
// and its items are one deeper. A context minted for the batch itself,
// whose ID namespace is the batch's ID, has the items as its own
// operations, so they have c's depth. The two are told apart by whether
// c's ID prefix is the batch's ID.
func (c *execContext) batchItemDepth(parentID string) int {
	if c.ids.prefix == parentID {
		return c.hookDepth
	}
	return c.hookDepth + 1
}

// hooksAt returns the dispatcher for the operation-level hooks of an
// operation at depth in the operation tree: the registered plugins when
// the depth is within the bound set by [WithPluginChildOperationsDepth],
// nil otherwise. A nil dispatcher makes every dispatch a no-op and every
// wrap chain run the wrapped work directly, so an operation beyond the
// bound runs and checkpoints exactly as one within it.
func (c *execContext) hooksAt(depth int) *pluginDispatcher {
	if c.hookDepthBound != 0 && depth >= c.hookDepthBound {
		return nil
	}
	return c.pluginDispatcher
}

// operationHooks returns the dispatcher for the operation-level hooks of
// an operation claimed on c; see hooksAt.
func (c *execContext) operationHooks() *pluginDispatcher {
	return c.hooksAt(c.hookDepth)
}

// childrenOmittedAt reports whether the operations nested inside an
// operation at depth are omitted from plugin notifications: true exactly
// when depth is the deepest depth [WithPluginChildOperationsDepth] reports.
// An operation deeper than that is not reported at all, so the value is
// false for it.
func (c *execContext) childrenOmittedAt(depth int) bool {
	return c.hookDepthBound != 0 && depth == c.hookDepthBound-1
}

// branch creates a sibling context for an async operation's goroutine. The
// operation's ID was already claimed from this context's ids before the
// goroutine started, so the branch shares ids, state, checkpointer,
// suspend, serdes and abandon with the caller: only blocked is fresh, which
// isolates a pending state to this branch and leaves the caller free to
// keep claiming operations. owner is the goroutine that runs the operation,
// captured after that goroutine starts. Unlike child, ids is shared by
// pointer so the parent-ID prefix is preserved and no nested operation-ID
// namespace is minted; the branch also keeps the caller's log scope, since
// it runs inside the same operation. The branch inherits the caller's token
// without owning it; every caller registers the goroutine's own token and
// adopts it through adoptBranchToken.
//
// branch copies c's serializer defaults and logging settings as they are at
// the call. The asynchronous operations launch their goroutine while the
// owner keeps running, so they use branchWith with a snapshot taken before
// the go statement; a ConfigureSerdes or ConfigureLogging call between the
// go statement and the branch's construction then does not reach the
// branch. See configureSerdes and configureLogging.
func (c *execContext) branch(owner goroutineOwner) *execContext {
	return c.branchWith(owner, c.inheritedDefaults())
}

// branchWith is branch with the inherited defaults supplied by the caller
// instead of read from c.
func (c *execContext) branchWith(owner goroutineOwner, d inheritedDefaults) *execContext {
	b := &execContext{
		Context:            c.Context,
		executionArn:       c.executionArn,
		invocation:         c.invocation,
		logScope:           c.logScope,
		ids:                c.ids,
		owner:              owner,
		state:              c.state,
		checkpointParent:   c.checkpointParent,
		suspend:            c.suspend,
		checkpointer:       c.checkpointer,
		pluginDispatcher:   c.pluginDispatcher,
		hookDepth:          c.hookDepth,
		hookDepthBound:     c.hookDepthBound,
		executionStartTime: c.executionStartTime,
		noStackTraces:      c.noStackTraces,
		abandon:            c.abandon,
		branchTok:          c.branchTok,
		combinatorObserve:  c.combinatorObserve,
	}
	b.setSerdesDefaults(d.serdes)
	b.setLogDefaults(d.log)
	b.mode.Store(c.mode.Load())
	b.attachLogger()
	return b
}
