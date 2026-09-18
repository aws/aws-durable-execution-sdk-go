package durable

import (
	"errors"
	"fmt"
	"reflect"
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

// WithChildSummary supplies a summary function for the result of a
// [RunInChildContext], [RunInChildContextAsync], or [Go] operation. O is
// the operation's result type; a mismatch is a configuration error the
// operation returns before it claims an operation ID.
//
// The SDK checkpoints the child's result when it is at most 256 KiB
// serialized. A larger result is not stored: the checkpoint records that
// the child's operations are kept, and replay re-executes the child body
// to rebuild the value. Without a summary that checkpoint carries no
// payload, so inspecting the execution history shows nothing about what
// the child produced. With a summary, the SDK calls fn with the result
// and stores the returned string as the checkpoint payload instead:
//
//	durable.RunInChildContext(ctx, "import", importRows,
//		durable.WithChildSummary(func(rows []Row) string {
//			return fmt.Sprintf("%d rows imported", len(rows))
//		}))
//
// fn runs only when the serialized result exceeds the limit, and only on
// the invocation that produced the result. The summary is advisory: the
// SDK never reads it back, and replay correctness never depends on it.
// The summary must itself fit the checkpoint limit. A summary longer than
// 256 KiB is truncated on a UTF-8 boundary to that size; an empty summary
// leaves the payload absent. A panic in fn fails the operation with an
// error naming the child. fn must be deterministic and free of side
// effects: it runs at most once per execution, so a summary that varies
// between runs is a defect a reader of the history cannot detect.
func WithChildSummary[O any](fn func(result O) string) ChildOption {
	return childOptionFunc(func(o *childOptions) { o.summary = fn })
}

type childOptions struct {
	serdes      Serdes
	errorMapper func(err *ChildContextError) error

	// summary is the [WithChildSummary] function, stored as any because
	// ChildOption is not generic. childSummaryFunc asserts it against the
	// operation's result type.
	summary any
}

// childSummaryFunc returns the summary function configured for a child
// whose result type is O, or nil when none is set. A function of another
// result type is a configuration error.
func childSummaryFunc[O any](name string, options childOptions) (func(O) string, error) {
	if options.summary == nil {
		return nil, nil
	}
	fn, ok := options.summary.(func(O) string)
	if !ok {
		return nil, fmt.Errorf("durable: child context %q: WithChildSummary function has type %T, want func(%v) string",
			name, options.summary, reflect.TypeFor[O]())
	}
	return fn, nil
}

// childSummaryPayload returns the checkpoint payload for a child result
// that exceeded the size limit: the summary of result, truncated to the
// limit, or nil when no summary function is set or the summary is empty.
// A panic in the summary function becomes an error so it fails the child
// operation instead of unwinding the caller, or, on the asynchronous
// path, the child's goroutine and with it the process.
func childSummaryPayload[O any](name string, summary func(O) string, result O) (payload *string, err error) {
	if summary == nil {
		return nil, nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("durable: child context %q: WithChildSummary function panicked: %v", name, r)
		}
	}()
	s := truncateUTF8(summary(result), checkpointSizeLimitBytes)
	if s == "" {
		return nil, nil
	}
	return aws.String(s), nil
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

	options := childOptions{serdes: ec.serdesDefaults().serdes}
	for _, o := range opts {
		o.applyChild(&options)
	}
	summary, err := childSummaryFunc[O](name, options)
	if err != nil {
		return zero, err
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
			// fn runs on the calling goroutine, as on the first run. A
			// failure is returned in the same shape as a first-run failure
			// so the caller sees one error type on every invocation.
			if op.childCtx.replayChildren {
				child := ec.child(id, name, ec.owner, modeReplaySucceededContext)
				result, fnErr := fn(child)
				if fnErr != nil {
					return zero, replayedChildFailure(name, options, fnErr, ec.returnedErrorTrace(fn, fnErr, 0))
				}
				return result, nil
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
	child := ec.child(id, name, ec.owner, mode)

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

	// From here to the checkpoint of the child's completion the operation
	// is an executing span: its outcome belongs to this invocation, so a
	// handler blocked on a pending operation waits for it to be recorded
	// before it suspends. See awaitDrain.
	ec.suspend.enterExecuting()
	defer ec.suspend.exitExecuting()

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
		// Large payload: checkpoint with ReplayChildren so the backend
		// preserves child operations for reconstruction. The payload is
		// the caller's summary, if any; replay never reads it.
		update.ContextOptions = &ContextOptions{ReplayChildren: aws.Bool(true)}
		update.Payload, err = childSummaryPayload(name, summary, result)
		if err != nil {
			return zero, err
		}
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

	options := childOptions{serdes: ec.serdesDefaults().serdes}
	for _, o := range opts {
		o.applyChild(&options)
	}
	summary, err := childSummaryFunc[O](name, options)
	if err != nil {
		return newFailedFuture[O](err)
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

	// Snapshot the serializer defaults on the owning goroutine: the owner
	// may call ConfigureSerdes before the goroutine below runs.
	defaults := ec.serdesDefaults()

	go func() {
		defer tok.release()
		// The child context is owned by this goroutine. Capture
		// ownership here, not on the parent goroutine.
		child := ec.childWith(id, name, currentGoroutineOwner(), mode, defaults)
		child.adoptBranchToken(tok)

		// Recover panics in the child function so they settle the
		// future as a failure rather than crashing the process.
		result, fnTrace, fnErr := runUserFunc(child, fn, fmt.Sprintf("durable: child context %q panicked", name), func() (O, error) {
			return fn(child)
		})

		// From here to the checkpoint of the child's completion the
		// operation is an executing span: its outcome belongs to this
		// invocation, so a handler blocked on a pending operation waits
		// for it to be recorded before it suspends. The span ends before
		// the branch token is released. See awaitDrain.
		ec.suspend.enterExecuting()
		defer ec.suspend.exitExecuting()

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
			payload, perr := childSummaryPayload(name, summary, result)
			if perr != nil {
				var zero O
				fut.settle(zero, perr)
				return
			}
			update.Payload = payload
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
// forwarded unchanged, so [WithChildSerdes] applies to the child result,
// [WithChildSummary] to its checkpoint when the result is oversized, and
// [WithChildErrorMapper] to its failure.
//
// The child's operation identity is claimed before Go returns, so
// consecutive Go calls from one goroutine are replay-deterministic. Inside
// fn, the provided Context is owned by fn's goroutine, and all durable
// operations on it are safe, including nested Go calls.
func Go[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) *Future[O] {
	return RunInChildContextAsync(ctx, name, fn, opts...)
}

// resolveTerminalChild handles a child operation that already has a terminal
// status in the checkpoint log. It returns a pre-settled future, except in
// ReplayChildren mode, where the child body runs again on its own goroutine
// and the future settles when it finishes.
func resolveTerminalChild[O any](ec *execContext, op *operation, id, name string, options childOptions, fn func(Context) (O, error)) *Future[O] {
	switch op.status {
	case statusSucceeded:
		if op.childCtx == nil {
			return newFailedFuture[O](fmt.Errorf("durable: child context %q: checkpointed %s operation has no context details", name, op.status))
		}
		// ReplayChildren mode: re-execute the child body to reconstruct
		// the large result that was not checkpointed.
		if op.childCtx.replayChildren {
			return replayChildAsync(ec, id, name, options, fn)
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

// replayChildAsync re-executes the body of a SUCCEEDED child context whose
// result was not checkpointed. The body runs on its own goroutine with its
// own branch token, the same shape as the first run of
// [RunInChildContextAsync], so the returned future is unsettled when this
// function returns and the child does not borrow the caller's branch
// registration. The child's completion is already recorded, so nothing is
// checkpointed; a failure settles the future in the same shape as a
// first-run failure.
func replayChildAsync[O any](ec *execContext, id, name string, options childOptions, fn func(Context) (O, error)) *Future[O] {
	fut := newFuture[O]()
	registerFuture(ec.suspend, fut)

	// Snapshot the serializer defaults on the owning goroutine: the owner
	// may call ConfigureSerdes before the goroutine below runs.
	defaults := ec.serdesDefaults()
	tok := ec.suspend.registerBranchToken()
	go func() {
		defer tok.release()
		child := ec.childWith(id, name, currentGoroutineOwner(), modeReplaySucceededContext, defaults)
		child.adoptBranchToken(tok)

		result, fnTrace, fnErr := runUserFunc(child, fn, fmt.Sprintf("durable: child context %q panicked", name), func() (O, error) {
			return fn(child)
		})
		if fnErr != nil {
			var zero O
			fut.settle(zero, replayedChildFailure(name, options, fnErr, fnTrace))
			return
		}
		fut.settle(result, nil)
	}()
	return fut
}

// replayedChildFailure builds the error returned when a child body fails
// while re-executing in ReplayChildren mode. Suspension is not a child
// failure: it propagates as errSuspendExecution so the invocation ends
// PENDING and the child resumes in a later invocation. Any other error is
// wrapped through the configured mapper exactly as a first-run failure is.
func replayedChildFailure(name string, options childOptions, fnErr error, fnTrace []string) error {
	if errors.Is(fnErr, errSuspendExecution) || errors.Is(fnErr, errCheckpointTerminated) {
		return errSuspendExecution
	}
	return options.failure(name, recordOf(fnErr).withTrace(fnTrace))
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
	if parent := ec.parentOperationID(); parent != "" {
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
