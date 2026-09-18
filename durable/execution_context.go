package durable

import (
	"context"
	"fmt"
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
	logger       Logger

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

	// serdes is the handler-level default serializer for operation
	// results. Per-operation serdes options take precedence.
	serdes Serdes

	// callbackDeserializer is the handler-level default deserializer for
	// callback payloads submitted by external systems. When nil, callbacks
	// use the standard serdes.
	callbackDeserializer Deserializer

	// suspend is the invocation-wide suspension signal, shared across
	// the root and all child contexts.
	suspend *suspendSignal

	// checkpointer persists operation updates; shared across the root and
	// all child contexts of one invocation.
	checkpointer *checkpointer

	// pluginDispatcher dispatches lifecycle hooks to registered plugins.
	// Nil when no plugins are registered (zero overhead).
	pluginDispatcher *pluginDispatcher

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
func newExecContext(ctx context.Context, executionArn string, inv invocationInfo, logger Logger, state *executionState) *execContext {
	mode := modeExecution
	if state.numOperations() > 1 {
		mode = modeReplay
	}
	// Sync logger replay state with initial mode.
	if toggler, ok := logger.(replayToggler); ok {
		toggler.setReplaying(mode == modeReplay || mode == modeReplaySucceededContext)
	}
	ec := &execContext{
		Context:      ctx,
		executionArn: executionArn,
		invocation:   inv,
		logger:       logger,
		ids:          &opIDs{},
		owner:        currentGoroutineOwner(),
		state:        state,
		serdes:       jsonSerdes{},
		suspend:      newSuspendSignal(),
	}
	ec.mode.Store(int32(mode))
	return ec
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

func (c *execContext) RequestID() string { return c.invocation.requestID }

func (c *execContext) InvokedFunctionARN() string { return c.invocation.invokedFunctionARN }

func (c *execContext) Logger() Logger { return c.logger }

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
	if c.suspend.fired() {
		return "", errSuspendExecution
	}
	if c.blocked.Load() {
		return "", errSuspendExecution
	}
	if c.abandon.abandoned() {
		return "", errSuspendExecution
	}
	if err := c.owner.check(); err != nil {
		return "", err
	}
	c.refreshReplayMode()
	return c.ids.next(), nil
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
	// Notify the logger that replay has ended.
	if toggler, ok := c.logger.(replayToggler); ok {
		toggler.setReplaying(false)
	}
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

// child creates the context for a child operation with the given entity ID.
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
func (c *execContext) child(entityID string, owner goroutineOwner, mode executionMode) *execContext {
	child := &execContext{
		Context:              c.Context,
		executionArn:         c.executionArn,
		invocation:           c.invocation,
		logger:               c.logger,
		ids:                  c.ids.child(entityID),
		owner:                owner,
		state:                c.state,
		checkpointParent:     entityID,
		serdes:               c.serdes,
		callbackDeserializer: c.callbackDeserializer,
		suspend:              c.suspend,
		checkpointer:         c.checkpointer,
		pluginDispatcher:     c.pluginDispatcher,
		executionStartTime:   c.executionStartTime,
		noStackTraces:        c.noStackTraces,
		abandon:              c.abandon,
		branchTok:            c.branchTok,
		combinatorObserve:    c.combinatorObserve,
	}
	child.mode.Store(int32(mode))
	return child
}

// virtualChild creates the context for a virtual child operation: one that
// mints its own operation-ID namespace under entityID but is never
// checkpointed itself. Because no operation with ID entityID exists in the
// checkpoint log, operations claimed on the virtual child record
// parentID, the nearest checkpointed ancestor, as their ParentId. The
// caller computes mode as for child.
func (c *execContext) virtualChild(entityID, parentID string, owner goroutineOwner, mode executionMode) *execContext {
	vc := c.child(entityID, owner, mode)
	vc.checkpointParent = parentID
	return vc
}

// branch creates a sibling context for an async operation's goroutine. The
// operation's ID was already claimed from this context's ids before the
// goroutine started, so the branch shares ids, state, checkpointer,
// suspend, serdes and abandon with the caller: only blocked is fresh, which
// isolates a pending state to this branch and leaves the caller free to
// keep claiming operations. owner is the goroutine that runs the operation,
// captured after that goroutine starts. Unlike child, ids is shared by
// pointer so the parent-ID prefix is preserved and no nested operation-ID
// namespace is minted. The branch inherits the caller's token without
// owning it; every caller registers the goroutine's own token and adopts
// it through adoptBranchToken.
func (c *execContext) branch(owner goroutineOwner) *execContext {
	b := &execContext{
		Context:              c.Context,
		executionArn:         c.executionArn,
		invocation:           c.invocation,
		logger:               c.logger,
		ids:                  c.ids,
		owner:                owner,
		state:                c.state,
		checkpointParent:     c.checkpointParent,
		serdes:               c.serdes,
		callbackDeserializer: c.callbackDeserializer,
		suspend:              c.suspend,
		checkpointer:         c.checkpointer,
		pluginDispatcher:     c.pluginDispatcher,
		executionStartTime:   c.executionStartTime,
		noStackTraces:        c.noStackTraces,
		abandon:              c.abandon,
		branchTok:            c.branchTok,
		combinatorObserve:    c.combinatorObserve,
	}
	b.mode.Store(c.mode.Load())
	return b
}
