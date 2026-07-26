package durable

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// operationSubTypeRunInChildContext is the wire subtype for child-context
// operations.
const operationSubTypeRunInChildContext = "RunInChildContext"

// checkpointSizeLimitBytes is the maximum checkpoint payload size (256KB).
// Payloads exceeding this trigger ReplayChildren mode: the context result
// is not stored in the checkpoint; instead the child body is re-executed
// on replay to reconstruct the value.
const checkpointSizeLimitBytes = 256 * 1024

// ChildOption configures a single child-context operation.
type ChildOption interface {
	applyChild(*childOptions)
}

// WithChildSerdes overrides the serializer for the child context's result.
func WithChildSerdes(s Serdes) ChildOption {
	return childOptionFunc(func(o *childOptions) { o.serdes = s })
}

type childOptions struct {
	serdes Serdes
}

type childOptionFunc func(*childOptions)

func (f childOptionFunc) applyChild(o *childOptions) { f(o) }

// RunInChildContext runs fn in a child context with isolated operation
// tracking. Use it to group durable operations into a named sub-workflow
// whose overall result is checkpointed: on replay of a completed child
// context, the stored result is returned without re-executing fn. If fn
// fails, RunInChildContext returns a [*ChildContextError].
func RunInChildContext[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) (O, error) {
	var zero O
	ec, ok := ctx.(*execContext)
	if !ok {
		return zero, fmt.Errorf("durable: RunInChildContext %q: Context was not created by the SDK", name)
	}

	options := childOptions{serdes: ec.serdes}
	for _, o := range opts {
		o.applyChild(&options)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}

	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), operationSubTypeRunInChildContext, name); err != nil {
		return zero, err
	}
	if op != nil {
		switch op.status {
		case statusSucceeded:
			if op.childCtx == nil {
				return zero, fmt.Errorf("durable: child context %q: checkpointed %s operation has no context details", name, op.status)
			}
			// ReplayChildren mode: the result was too large to
			// checkpoint, so re-execute the child body to reconstruct it.
			if op.childCtx.replayChildren {
				mode := modeReplaySucceededContext
				child := ec.child(id, ec.owner, mode)
				return fn(child)
			}
			var out O
			if err := options.serdes.Unmarshal(ec.serdesCtx(id), []byte(op.childCtx.result), &out); err != nil {
				return zero, fmt.Errorf("durable: child context %q: deserialize result: %w", name, err)
			}
			return out, nil

		case statusFailed:
			cause := &replayedError{errType: "Error", message: "child context failed"}
			if op.childCtx != nil {
				cause = &replayedError{errType: op.childCtx.errType, message: op.childCtx.errMessage}
			}
			return zero, &ChildContextError{Name: name, Err: cause}

		case statusStarted, statusPending, statusReady, statusCancelled, statusTimedOut, statusStopped:
			// STARTED re-enters below and replays the child's own
			// operations. The remaining statuses are not produced
			// for context operations.
		}
	}

	if op == nil {
		update := childUpdate(ec, id, name, types.OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
			return zero, err
		}
	}

	// The child context mints IDs under its own entity ID. It replays
	// when its first operation is already checkpointed. fn runs on the
	// calling goroutine, which owns the child context.
	mode := childReplayMode(ec, id, op)
	child := ec.child(id, ec.owner, mode)

	opInfo := OperationHookInfo{
		ExecutionArn:   ec.executionArn,
		ID:             id,
		Name:           name,
		Type:           string(types.OperationTypeContext),
		SubType:        operationSubTypeRunInChildContext,
		Status:         PluginOperationStarted,
		IsReplay:       ec.IsReplaying(),
		ParentID:       ec.parentWireID(),
		StartTimestamp: time.Now(),
	}

	// WrapChildContextFn wraps the child body execution.
	wrappedResult, wrappedErr := wrapChain(ec.pluginDispatcher,
		func(p *Plugin) func(func() (any, error)) (any, error) {
			if p.WrapChildContextFn == nil {
				return nil
			}
			return func(innerFn func() (any, error)) (any, error) {
				return p.WrapChildContextFn(ec, opInfo, innerFn)
			}
		},
		func() (any, error) {
			return fn(child)
		},
	)

	var result O
	var fnErr error
	if wrappedErr != nil {
		fnErr = wrappedErr
	} else if wrappedResult != nil {
		result, _ = wrappedResult.(O)
	}

	if fnErr != nil {
		// Suspension is not a child failure: it propagates so the
		// invocation ends PENDING and the child resumes in a later
		// invocation.
		if errors.Is(fnErr, errSuspendExecution) {
			return zero, fnErr
		}
		update := childUpdate(ec, id, name, types.OperationActionFail)
		update.Error = errorObject(fnErr)
		if cerr := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); cerr != nil {
			return zero, cerr
		}
		return zero, &ChildContextError{Name: name, Err: fnErr}
	}

	serialized, err := options.serdes.Marshal(ec.serdesCtx(id), result)
	if err != nil {
		return zero, fmt.Errorf("durable: child context %q: serialize result: %w", name, err)
	}
	update := childUpdate(ec, id, name, types.OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		// Large payload: checkpoint with empty payload and ReplayChildren
		// so the backend preserves child operations for reconstruction.
		update.ContextOptions = &types.ContextOptions{ReplayChildren: aws.Bool(true)}
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return zero, err
	}

	var out O
	if err := options.serdes.Unmarshal(ec.serdesCtx(id), serialized, &out); err != nil {
		return zero, fmt.Errorf("durable: child context %q: deserialize result: %w", name, err)
	}
	return out, nil
}

// RunInChildContextAsync is [RunInChildContext], except that fn runs
// concurrently on its own goroutine and the result is delivered through the
// returned future.
//
// The child's operation identity is claimed synchronously before
// RunInChildContextAsync returns, preserving deterministic program order.
// Inside fn, the provided Context is owned by fn's goroutine, and all
// durable operations on it are safe, including nested Go calls.
//
// On invocation suspension, the returned future is settled with
// errSuspendExecution so goroutines blocked on [Future.Result] unwind.
func RunInChildContextAsync[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) *Future[O] {
	ec, ok := ctx.(*execContext)
	if !ok {
		return newFailedFuture[O](fmt.Errorf("durable: RunInChildContextAsync %q: Context was not created by the SDK", name))
	}

	options := childOptions{serdes: ec.serdes}
	for _, o := range opts {
		o.applyChild(&options)
	}

	// Claim the operation ID synchronously on the calling goroutine to
	// preserve deterministic ID minting order across concurrent Go calls.
	id, err := ec.claimOperation()
	if err != nil {
		return newFailedFuture[O](err)
	}

	// Check if the operation is already checkpointed (terminal).
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), operationSubTypeRunInChildContext, name); err != nil {
		return newFailedFuture[O](err)
	}
	if op != nil && op.status.terminal() {
		return resolveTerminalChild[O](ec, op, id, name, options, fn)
	}

	// Checkpoint START if this is the first invocation of this child.
	if op == nil {
		update := childUpdate(ec, id, name, types.OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
			return newFailedFuture[O](err)
		}
	}

	// Create the future and register it for suspension settlement before
	// launching the goroutine. This ordering guarantees that a suspension
	// fired between here and the goroutine start settles the future.
	fut := newFuture[O]()
	registerFuture(ec.suspend, fut)

	// Determine child replay mode before launching the goroutine. This
	// uses the same logic as the blocking variant.
	mode := childReplayMode(ec, id, op)

	// Register the child goroutine as an active branch before launching.
	// It deregisters (via defer) after settling the future on all paths.
	ec.suspend.registerBranch()

	go func() {
		defer ec.suspend.deregisterBranch()
		// The child context is owned by this goroutine. Capture
		// ownership here, not on the parent goroutine.
		child := ec.child(id, currentGoroutineOwner(), mode)

		var result O
		var fnErr error

		// Recover panics in the child function so they settle the
		// future as a failure rather than crashing the process.
		func() {
			defer func() {
				if r := recover(); r != nil {
					fnErr = fmt.Errorf("durable: child context %q panicked: %v", name, r)
				}
			}()
			result, fnErr = fn(child)
		}()

		if fnErr != nil {
			// Suspension propagates: settle with suspension error
			// so the parent sees suspension, not a child failure.
			if errors.Is(fnErr, errSuspendExecution) {
				fut.settle(result, errSuspendExecution)
				return
			}
			// Checkpoint the failure. If checkpointing fails, the
			// settle error is the checkpoint failure.
			update := childUpdate(ec, id, name, types.OperationActionFail)
			update.Error = errorObject(fnErr)
			if cerr := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); cerr != nil {
				fut.settle(result, cerr)
				return
			}
			var zero O
			fut.settle(zero, &ChildContextError{Name: name, Err: fnErr})
			return
		}

		serialized, serr := options.serdes.Marshal(ec.serdesCtx(id), result)
		if serr != nil {
			fut.settle(result, fmt.Errorf("durable: child context %q: serialize result: %w", name, serr))
			return
		}
		update := childUpdate(ec, id, name, types.OperationActionSucceed)
		if len(serialized) > checkpointSizeLimitBytes {
			update.ContextOptions = &types.ContextOptions{ReplayChildren: aws.Bool(true)}
		} else {
			update.Payload = aws.String(string(serialized))
		}
		if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
			fut.settle(result, err)
			return
		}

		// Round-trip through serdes for consistency with the blocking
		// variant (first-run value == replay value).
		var out O
		if err := options.serdes.Unmarshal(ec.serdesCtx(id), serialized, &out); err != nil {
			fut.settle(result, fmt.Errorf("durable: child context %q: deserialize result: %w", name, err))
			return
		}
		fut.settle(out, nil)
	}()

	return fut
}

// Go runs fn concurrently in its own child context and returns a future for
// its result. It is the replay-safe substitute for the go statement inside
// durable functions.
//
// The child's operation identity is claimed before Go returns, so
// consecutive Go calls from one goroutine are replay-deterministic. Inside
// fn, the provided Context is owned by fn's goroutine, and all durable
// operations on it are safe, including nested Go calls.
func Go[O any](ctx Context, name string, fn func(Context) (O, error)) *Future[O] {
	return RunInChildContextAsync(ctx, name, fn)
}

// resolveTerminalChild handles a child operation that already has a terminal
// status in the checkpoint log, returning a pre-settled future.
func resolveTerminalChild[O any](ec *execContext, op *operation, id, name string, options childOptions, fn func(Context) (O, error)) *Future[O] {
	switch op.status {
	case statusSucceeded:
		if op.childCtx == nil {
			return newFailedFuture[O](fmt.Errorf("durable: child context %q: checkpointed %s operation has no context details", name, op.status))
		}
		// ReplayChildren mode: re-execute the child body to reconstruct
		// the large result that was not checkpointed.
		if op.childCtx.replayChildren {
			mode := modeReplaySucceededContext
			child := ec.child(id, currentGoroutineOwner(), mode)
			result, err := fn(child)
			if err != nil {
				return newFailedFuture[O](err)
			}
			return newSettledFuture(result, nil)
		}
		var out O
		if err := options.serdes.Unmarshal(ec.serdesCtx(id), []byte(op.childCtx.result), &out); err != nil {
			return newFailedFuture[O](fmt.Errorf("durable: child context %q: deserialize result: %w", name, err))
		}
		return newSettledFuture(out, nil)

	case statusFailed:
		cause := &replayedError{errType: "Error", message: "child context failed"}
		if op.childCtx != nil {
			cause = &replayedError{errType: op.childCtx.errType, message: op.childCtx.errMessage}
		}
		return newFailedFuture[O](&ChildContextError{Name: name, Err: cause})

	default:
		// CANCELLED, TIMED_OUT, STOPPED: not expected for context ops,
		// but handle gracefully.
		return newFailedFuture[O](fmt.Errorf("durable: child context %q: unexpected terminal status %s", name, op.status))
	}
}

// childReplayMode determines the execution mode for a child context. A child
// replays when its first operation is already checkpointed (probe id+"-1").
// A child whose overall result is SUCCEEDED uses modeReplaySucceededContext
// so in-flight operations within it block rather than re-execute.
func childReplayMode(ec *execContext, id string, op *operation) executionMode {
	if op != nil && op.status == statusSucceeded {
		return modeReplaySucceededContext
	}
	if ec.state.get(id+"-1") != nil {
		return modeReplay
	}
	return modeExecution
}

// childUpdate assembles the shared fields of a child-context operation
// update. IDs are hashed to their wire form.
func childUpdate(ec *execContext, id, name string, action types.OperationAction) types.OperationUpdate {
	update := types.OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    types.OperationTypeContext,
		SubType: aws.String(operationSubTypeRunInChildContext),
		Action:  action,
	}
	if name != "" {
		update.Name = aws.String(name)
	}
	if parent := ec.ids.prefix; parent != "" {
		update.ParentId = aws.String(hashID(parent))
	}
	return update
}
