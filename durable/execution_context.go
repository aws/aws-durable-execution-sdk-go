package durable

import (
	"context"

	"github.com/aws/aws-lambda-go/lambdacontext"
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
	// modeReplay, an operation with no terminal checkpoint blocks
	// indefinitely instead of flipping the context to live execution.
	modeReplaySucceededContext
)

// execContext is the concrete [Context] implementation.
type execContext struct {
	context.Context

	executionArn string
	lambdaCtx    *lambdacontext.LambdaContext
	logger       Logger

	mode  executionMode
	ids   *opIDs
	owner goroutineOwner
	state *executionState

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
}

var _ Context = (*execContext)(nil)

// newExecContext creates the root context for one invocation. The mode
// starts in replay when checkpointed operations beyond the always-present
// execution operation exist.
func newExecContext(ctx context.Context, executionArn string, lambdaCtx *lambdacontext.LambdaContext, logger Logger, state *executionState) *execContext {
	mode := modeExecution
	if len(state.operations) > 1 {
		mode = modeReplay
	}
	// Sync logger replay state with initial mode.
	if toggler, ok := logger.(replayToggler); ok {
		toggler.setReplaying(mode == modeReplay || mode == modeReplaySucceededContext)
	}
	return &execContext{
		Context:      ctx,
		executionArn: executionArn,
		lambdaCtx:    lambdaCtx,
		logger:       logger,
		mode:         mode,
		ids:          &opIDs{},
		owner:        currentGoroutineOwner(),
		state:        state,
		serdes:       jsonSerdes{},
		suspend:      newSuspendSignal(),
	}
}

func (c *execContext) ExecutionArn() string { return c.executionArn }

func (c *execContext) LambdaContext() *lambdacontext.LambdaContext { return c.lambdaCtx }

func (c *execContext) Logger() Logger { return c.logger }

func (c *execContext) IsReplaying() bool {
	return c.mode == modeReplay || c.mode == modeReplaySucceededContext
}

// parentWireID returns the hashed wire-format parent context ID for use in
// OperationHookInfo.ParentID. Returns empty string for root-level operations.
func (c *execContext) parentWireID() string {
	if c.ids.prefix == "" {
		return ""
	}
	return hashID(c.ids.prefix)
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
	if c.mode != modeReplay {
		return
	}
	pendingID := c.ids.peek()
	if c.state.get(pendingID) != nil {
		return
	}
	if c.state.get(pendingID+"-1") != nil {
		return
	}
	c.mode = modeExecution
	// Notify the logger that replay has ended.
	if toggler, ok := c.logger.(replayToggler); ok {
		toggler.setReplaying(false)
	}
}

// child creates the context for a child operation with the given entity ID.
//
// The caller is responsible for computing mode: a child whose own operation
// is already checkpointed as SUCCEEDED must receive
// modeReplaySucceededContext, not the parent's current mode. Owner is the
// goroutine that runs the child function, captured by the caller after that
// goroutine starts.
func (c *execContext) child(entityID string, owner goroutineOwner, mode executionMode) *execContext {
	return &execContext{
		Context:              c.Context,
		executionArn:         c.executionArn,
		lambdaCtx:            c.lambdaCtx,
		logger:               c.logger,
		mode:                 mode,
		ids:                  c.ids.child(entityID),
		owner:                owner,
		state:                c.state,
		serdes:               c.serdes,
		callbackDeserializer: c.callbackDeserializer,
		suspend:              c.suspend,
		checkpointer:         c.checkpointer,
		pluginDispatcher:     c.pluginDispatcher,
	}
}
