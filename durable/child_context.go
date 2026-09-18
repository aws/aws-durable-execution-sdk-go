package durable

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

// WithChildErrorMapper supplies a function that maps a child context's
// failure before [RunInChildContext], [RunInChildContextAsync], or [Go]
// returns it. Without a mapper a failed child returns a
// [*ChildContextError]. With a mapper, that same [*ChildContextError] is
// passed to mapper and mapper's result is returned instead. A nil result
// is ignored and the [*ChildContextError] is returned unchanged, so a
// failure cannot become a success by mistake.
//
// Mapper runs on every invocation that reaches the failed child: on the
// first invocation after the body fails, and on each replay, where the
// body does not run. Its input is the same each time. The
// [*ChildContextError] is built from the recorded failure, never from the
// live error value, so the ErrorType, Message, ErrorData, and StackTrace
// mapper sees on the first invocation are the ones it sees on replay.
// Mapper must therefore be deterministic: the same input yields the same
// output, with no dependence on time, randomness, or state outside the
// error. A deterministic mapper reproduces the mapped error on replay:
//
//	durable.WithChildErrorMapper(func(err *durable.ChildContextError) error {
//		if err.ErrorType == "StepError" {
//			return &PaymentError{Reason: err.Message}
//		}
//		return err
//	})
//
// The checkpoint records the failure that escaped the child body, which
// is mapper's input, not mapper's result. A result of the handler's own
// type cannot be rebuilt from a record, so recording the input and
// mapping it again is what reproduces the mapped error on replay. A
// mapper that alters the fields of the [*ChildContextError] it receives,
// or returns a new one, changes what the handler sees but not what is
// recorded. The execution history therefore shows the original failure.
func WithChildErrorMapper(mapper func(err *ChildContextError) error) ChildOption {
	return childOptionFunc(func(o *childOptions) { o.errorMapper = mapper })
}

type childOptions struct {
	serdes      Serdes
	errorMapper func(err *ChildContextError) error
}

type childOptionFunc func(*childOptions)

func (f childOptionFunc) applyChild(o *childOptions) { f(o) }

// failure builds the error a failed child context returns from the record
// of the error that escaped its body, applying the configured mapper. rec
// is what the checkpoint stores; on the first invocation the caller
// checkpoints it, on replay it is the stored record. Building the
// [*ChildContextError] from rec on both paths gives the mapper one input.
func (o *childOptions) failure(name string, rec errorRecord) error {
	childErr := newChildContextError(name, rec)
	if o.errorMapper == nil {
		return childErr
	}
	if mapped := o.errorMapper(childErr); mapped != nil {
		return mapped
	}
	return childErr
}

// RunInChildContext runs fn in a child context with isolated operation
// tracking. Use it to group durable operations into a named sub-workflow
// whose overall result is checkpointed: on replay of a completed child
// context, the stored result is returned without re-executing fn. If fn
// fails, RunInChildContext returns a [*ChildContextError], or the error
// a [WithChildErrorMapper] mapper derives from it.
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
	if err := validateReplayConsistency(op, string(OperationTypeContext), operationSubTypeRunInChildContext, name); err != nil {
		return zero, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return zero, ec.parkUnfinishedReplay(op, id, string(OperationTypeContext), operationSubTypeRunInChildContext, name)
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
			if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.childCtx.result), &out); err != nil {
				return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
			}
			return out, nil

		case statusFailed:
			return zero, options.failure(name, childFailureRecord(op))

		case statusStarted, statusPending, statusReady, statusCancelled, statusTimedOut, statusStopped:
			// STARTED re-enters below and replays the child's own
			// operations. The remaining statuses are not produced
			// for context operations.
		}
	}

	if op == nil {
		update := childUpdate(ec, id, name, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			if errors.Is(err, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
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
		Type:           string(OperationTypeContext),
		SubType:        operationSubTypeRunInChildContext,
		Status:         PluginOperationStarted,
		IsReplay:       ec.IsReplaying(),
		ParentID:       ec.parentWireID(),
		StartTimestamp: time.Now(),
	}

	// WrapChildContextFn wraps the child body execution.
	var fnTrace []string
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
			r, e := fn(child)
			if e != nil {
				fnTrace = ec.returnedErrorTrace(fn, e, 0)
			}
			return r, e
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
		if errors.Is(fnErr, errSuspendExecution) || errors.Is(fnErr, errCheckpointTerminated) {
			return zero, errSuspendExecution
		}
		rec := recordOf(fnErr).withTrace(fnTrace)
		update := childUpdate(ec, id, name, OperationActionFail)
		update.Error = errorObjectFromRecord(rec)
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			if errors.Is(cerr, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
			return zero, cerr
		}
		return zero, options.failure(name, rec)
	}

	serialized, err := options.serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
	if err != nil {
		return zero, newSerdesError(name, serdesDirectionMarshal, err)
	}
	update := childUpdate(ec, id, name, OperationActionSucceed)
	if len(serialized) > checkpointSizeLimitBytes {
		// Large payload: checkpoint with empty payload and ReplayChildren
		// so the backend preserves child operations for reconstruction.
		update.ContextOptions = &ContextOptions{ReplayChildren: aws.Bool(true)}
	} else {
		update.Payload = aws.String(string(serialized))
	}
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
		if errors.Is(err, errCheckpointTerminated) {
			return zero, errSuspendExecution
		}
		return zero, err
	}

	var out O
	if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), serialized, &out); err != nil {
		return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
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
// durable operations on it are safe, including nested Go calls. If fn
// fails, the future settles with a [*ChildContextError], or the error a
// [WithChildErrorMapper] mapper derives from it.
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
	if err := validateReplayConsistency(op, string(OperationTypeContext), operationSubTypeRunInChildContext, name); err != nil {
		return newFailedFuture[O](err)
	}
	if ec.unfinishedInSucceededContext(op) {
		return newUnfinishedReplayFuture[O](ec.suspend)
	}
	if op != nil && op.status.terminal() {
		return resolveTerminalChild[O](ec, op, id, name, options, fn)
	}

	// Checkpoint START if this is the first invocation of this child.
	if op == nil {
		update := childUpdate(ec, id, name, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			if errors.Is(err, errCheckpointTerminated) {
				return newFailedFuture[O](errSuspendExecution)
			}
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
	tok := ec.suspend.registerBranchToken()

	go func() {
		defer tok.release()
		// The child context is owned by this goroutine. Capture
		// ownership here, not on the parent goroutine.
		child := ec.child(id, currentGoroutineOwner(), mode)
		child.adoptBranchToken(tok)

		// Recover panics in the child function so they settle the
		// future as a failure rather than crashing the process.
		result, fnTrace, fnErr := runUserFunc(child, fn, fmt.Sprintf("durable: child context %q panicked", name), func() (O, error) {
			return fn(child)
		})

		if fnErr != nil {
			// Suspension propagates: settle with suspension error
			// so the parent sees suspension, not a child failure.
			if errors.Is(fnErr, errSuspendExecution) || errors.Is(fnErr, errCheckpointTerminated) {
				var zero O
				fut.settle(zero, errSuspendExecution)
				return
			}
			// Checkpoint the failure. If checkpointing fails, the
			// settle error is the checkpoint failure.
			rec := recordOf(fnErr).withTrace(fnTrace)
			update := childUpdate(ec, id, name, OperationActionFail)
			update.Error = errorObjectFromRecord(rec)
			if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
				// Terminated checkpointer means the invocation is
				// answering PENDING; treat as suspension.
				if errors.Is(cerr, errCheckpointTerminated) {
					var zero O
					fut.settle(zero, errSuspendExecution)
					return
				}
				fut.settle(result, cerr)
				return
			}
			var zero O
			fut.settle(zero, options.failure(name, rec))
			return
		}

		serialized, serr := options.serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
		if serr != nil {
			fut.settle(result, newSerdesError(name, serdesDirectionMarshal, serr))
			return
		}
		update := childUpdate(ec, id, name, OperationActionSucceed)
		if len(serialized) > checkpointSizeLimitBytes {
			update.ContextOptions = &ContextOptions{ReplayChildren: aws.Bool(true)}
		} else {
			update.Payload = aws.String(string(serialized))
		}
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			// Terminated checkpointer means the invocation is answering
			// PENDING; treat as suspension.
			if errors.Is(err, errCheckpointTerminated) {
				var zero O
				fut.settle(zero, errSuspendExecution)
				return
			}
			fut.settle(result, err)
			return
		}

		// Round-trip through serdes for consistency with the blocking
		// variant (first-run value == replay value).
		var out O
		if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), serialized, &out); err != nil {
			fut.settle(result, newSerdesError(name, serdesDirectionUnmarshal, err))
			return
		}
		fut.settle(out, nil)
	}()

	return fut
}

// Go runs fn concurrently in its own child context and returns a future for
// its result. It is the replay-safe substitute for the go statement inside
// durable functions, and shorthand for [RunInChildContextAsync]: opts are
// forwarded unchanged, so [WithChildSerdes] applies to the child result
// and [WithChildErrorMapper] to its failure.
//
// The child's operation identity is claimed before Go returns, so
// consecutive Go calls from one goroutine are replay-deterministic. Inside
// fn, the provided Context is owned by fn's goroutine, and all durable
// operations on it are safe, including nested Go calls.
func Go[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) *Future[O] {
	return RunInChildContextAsync(ctx, name, fn, opts...)
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
		if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.childCtx.result), &out); err != nil {
			return newFailedFuture[O](newSerdesError(name, serdesDirectionUnmarshal, err))
		}
		return newSettledFuture(out, nil)

	case statusFailed:
		return newFailedFuture[O](options.failure(name, childFailureRecord(op)))

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
func childUpdate(ec *execContext, id, name string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeContext,
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

// childFailureRecord returns the failure recorded for a FAILED child
// context, or a generic record when the checkpoint carries no details.
func childFailureRecord(op *operation) errorRecord {
	if op.childCtx == nil {
		return errorRecord{errType: "Error", message: "child context failed"}
	}
	return op.childCtx.record()
}
