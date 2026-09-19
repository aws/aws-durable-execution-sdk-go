package durable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

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

// WithChildSubType sets the operation subtype recorded for a
// [RunInChildContext], [RunInChildContextAsync], or [Go] operation.
// Without it the operation records [OperationSubTypeRunInChildContext].
// The subtype is written to the checkpoint and reported in
// [OperationHookInfo].SubType, so a plugin or a reader of the execution
// history can tell one kind of caller-defined grouping from another:
//
//	durable.RunInChildContext(ctx, "order-42", processOrder,
//		durable.WithChildSubType("OrderSaga"))
//
// subType must be 1 to 32 characters from the set A-Z, a-z, 0-9, hyphen,
// and underscore; an empty subType selects the default. The subtypes the
// SDK records for its own operations, the OperationSubType constants
// other than [OperationSubTypeRunInChildContext], are reserved: a child
// context cannot be labelled as a [Step], a [Map] iteration, or any other
// SDK operation, because plugins and tooling identify those operations by
// subtype alone. A value outside these rules is a configuration error the
// operation returns before it claims an operation ID.
//
// The subtype is part of the operation's identity on replay. Every
// invocation of the execution must supply the same subtype for the same
// operation; an invocation that finds a different subtype in the
// checkpoint returns a [*NonDeterministicReplayError]. A subtype must
// therefore not depend on the input, on time, or on any other value that
// can differ between invocations, and changing it in a deployment breaks
// the executions that are in flight.
func WithChildSubType(subType string) ChildOption {
	return childOptionFunc(func(o *childOptions) { o.subType = subType })
}

// WithChildVirtual makes a [RunInChildContext], [RunInChildContextAsync],
// or [Go] child context virtual. A virtual child context groups durable
// operations and scopes their names and log records like a checkpointed
// child context, but it is not an operation itself: nothing is
// checkpointed for the wrapper, so the execution history holds no
// ContextStarted, ContextSucceeded, or ContextFailed event for it. The
// operations inside it are checkpointed
// where the enclosing context's own operations are: they record the
// nearest checkpointed ancestor as their parent, and their IDs are
// numbered under the virtual child's position, so adding or removing the
// option around existing operations changes their identity on replay.
//
//	durable.RunInChildContext(ctx, "enrich", enrichOrder, durable.WithChildVirtual())
//
// This is the same mechanism [NestingFlat] applies to the items of a
// [Map] or [Parallel]: a flat item is a virtual child context that the
// batch creates for each item, and WithChildVirtual creates one
// standalone. Where [WithNesting] chooses per batch, WithChildVirtual
// chooses per child context.
//
// Because the wrapper leaves no record, replay re-runs the child body on
// every invocation that reaches it; the operations inside replay from
// their own checkpoints as usual, and a suspending operation inside it
// resumes in a later invocation exactly as it would in a checkpointed
// child. The body must therefore be deterministic in the same way a
// handler is. The child starts in the replay state of its parent, and
// like its parent it switches to live execution at its first operation
// that has no checkpoint. The result is round-tripped through the child's
// [Serdes] on every run, so the caller sees the same value live and on
// replay, and it has no size limit because it is never stored. A failure
// of the body is returned as a [*ChildContextError], or the error a
// [WithChildErrorMapper] mapper derives from it, rebuilt from the same
// failure on every run.
//
// Plugins observe a virtual child context as they observe a checkpointed
// one: an operation start is dispatched before [Plugin].WrapChildContextFn
// wraps the body, and an operation end once the body has an outcome, with
// the [WithChildSubType] subtype and the enclosing context's ParentID. A
// plugin that counts runs sees one start and one end per invocation that
// reaches the child. Because nothing is recorded, no event of the child
// records a checkpoint: the start and the end report IsReplay true on
// every invocation. WrapChildContextFn receives the start's info with
// IsReplay false when the child executes live and true when it replays the
// operations inside it. The operations inside it report the enclosing
// context's ParentID, not the child's, so the child adds no level to the
// depth [WithPluginChildOperationsDepth] counts. A [WithChildSummary]
// function is never called.
//
// A virtual child context cannot hold another virtual child context: the
// inner one returns a configuration error before it claims an operation
// ID. Make one of the two a checkpointed child context instead. A virtual
// child context inside a [NestingFlat] item, and a flat batch inside a
// virtual child context, are both supported.
func WithChildVirtual() ChildOption {
	return childOptionFunc(func(o *childOptions) { o.virtual = true })
}

type childOptions struct {
	serdes      Serdes
	errorMapper func(err *ChildContextError) error

	// summary is the [WithChildSummary] function, stored as any because
	// ChildOption is not generic. childSummaryFunc asserts it against the
	// operation's result type.
	summary any

	// subType is the [WithChildSubType] value; empty when the option was
	// not supplied. childSubType resolves and validates it.
	subType string

	// virtual is set by [WithChildVirtual]: the child is not checkpointed
	// itself. See runVirtualChild and runVirtualChildAsync.
	virtual bool
}

// maxOperationSubTypeLength is the longest operation subtype the service
// accepts.
const maxOperationSubTypeLength = 32

// childSubType returns the subtype a child context named name records:
// the default when the option was not supplied, else the configured value
// once it is validated. A value the service would reject, or one reserved
// for an SDK operation, is a configuration error.
func childSubType(name string, options childOptions) (string, error) {
	s := options.subType
	if s == "" {
		return OperationSubTypeRunInChildContext, nil
	}
	if len(s) > maxOperationSubTypeLength {
		return "", fmt.Errorf("durable: child context %q: WithChildSubType value is %d characters, the limit is %d",
			name, len(s), maxOperationSubTypeLength)
	}
	for i := 0; i < len(s); i++ {
		if !isSubTypeChar(s[i]) {
			return "", fmt.Errorf("durable: child context %q: WithChildSubType value %q has character %q at index %d, want A-Z, a-z, 0-9, hyphen, or underscore",
				name, s, s[i], i)
		}
	}
	if isReservedChildSubType(s) {
		return "", fmt.Errorf("durable: child context %q: WithChildSubType value %q is the subtype of an SDK operation and is reserved",
			name, s)
	}
	return s, nil
}

// isSubTypeChar reports whether c is a byte the service accepts in an
// operation subtype. The accepted set is ASCII, so checking bytes also
// rejects every multi-byte UTF-8 sequence.
func isSubTypeChar(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_'
}

// isReservedChildSubType reports whether s is a subtype the SDK records
// for an operation of its own. [OperationSubTypeRunInChildContext] is the
// default for a child context and is not reserved.
func isReservedChildSubType(s string) bool {
	switch s {
	case OperationSubTypeStep,
		OperationSubTypeWait,
		OperationSubTypeCallback,
		OperationSubTypeChainedInvoke,
		OperationSubTypeWaitForCallback,
		OperationSubTypeWaitForCondition,
		OperationSubTypeMap,
		OperationSubTypeMapIteration,
		OperationSubTypeParallel,
		OperationSubTypeParallelBranch:
		return true
	}
	return false
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
	subType, err := childSubType(name, options)
	if err != nil {
		return zero, err
	}
	if options.virtual {
		return runVirtualChild(ec, name, subType, options, fn)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}

	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeContext), subType, name); err != nil {
		return zero, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return zero, ec.parkUnfinishedReplay(op, id, string(OperationTypeContext), subType, name)
	}
	if op != nil {
		switch op.status {
		case statusSucceeded:
			if op.childCtx == nil {
				return zero, fmt.Errorf("durable: child context %q: checkpointed %s operation has no context details", name, op.status)
			}
			// The child reached its terminal state when the checkpoint
			// recorded it, so its end is dispatched before the result is
			// rebuilt: neither a failing result Serdes nor a failing
			// re-execution suppresses it.
			dispatchReplayedContextEnd(ec, id, name, subType, op, nil)
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
			failure := options.failure(name, childFailureRecord(op))
			dispatchReplayedContextEnd(ec, id, name, subType, op, failure)
			return zero, failure

		case statusStarted, statusPending, statusReady, statusCancelled, statusTimedOut, statusStopped:
			// STARTED re-enters below and replays the child's own
			// operations. The remaining statuses are not produced
			// for context operations.
		}
	}

	if op == nil {
		update := childUpdate(ec, id, name, subType, OperationActionStart)
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

	// The start is dispatched before the wrap hooks run, with the same
	// info the hooks receive, so a plugin can correlate the two.
	opInfo := dispatchContextStart(ec, id, name, subType, op)

	// WrapChildContextFn wraps the child body execution. The context the
	// hooks supply becomes the child context's parent; the child is not
	// visible to any other goroutine before fn runs.
	var fnTrace []string
	wrappedResult, wrappedErr := wrapChain(ec.operationHooks(), ec,
		func(p *Plugin) wrapHook {
			if p.WrapChildContextFn == nil {
				return nil
			}
			return func(ctx context.Context, innerFn wrapBody) (any, error) {
				return p.WrapChildContextFn(ctx, opInfo, innerFn)
			}
		},
		func(ctx context.Context) (any, error) {
			child.Context = ctx
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
		update := childUpdate(ec, id, name, subType, OperationActionFail)
		update.Error = errorObjectFromRecord(rec)
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			if errors.Is(cerr, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
			return zero, cerr
		}
		failure := options.failure(name, rec)
		dispatchContextEnd(ec, opInfo, "", failure)
		return zero, failure
	}

	serialized, err := options.serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
	if err != nil {
		return zero, newSerdesError(name, serdesDirectionMarshal, err)
	}
	update := childUpdate(ec, id, name, subType, OperationActionSucceed)
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
	dispatchContextEnd(ec, opInfo, aws.ToString(update.Payload), nil)

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
	subType, err := childSubType(name, options)
	if err != nil {
		return newFailedFuture[O](err)
	}
	if options.virtual {
		return runVirtualChildAsync(ec, name, subType, options, fn)
	}

	// Claim the operation ID synchronously on the calling goroutine to
	// preserve deterministic ID minting order across concurrent Go calls.
	id, err := ec.claimOperation()
	if err != nil {
		return newFailedFuture[O](err)
	}

	// Check if the operation is already checkpointed (terminal).
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeContext), subType, name); err != nil {
		return newFailedFuture[O](err)
	}
	if ec.unfinishedInSucceededContext(op) {
		return newUnfinishedReplayFuture[O](ec.suspend)
	}
	if op != nil && op.status.terminal() {
		return resolveTerminalChild[O](ec, op, id, name, subType, options, fn)
	}

	// Checkpoint START if this is the first invocation of this child.
	if op == nil {
		update := childUpdate(ec, id, name, subType, OperationActionStart)
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

	// Snapshot the serializer and logging defaults on the owning goroutine:
	// the owner may call ConfigureSerdes or ConfigureLogging before the
	// goroutine below runs.
	defaults := ec.inheritedDefaults()

	go func() {
		defer tok.release()
		// The child context is owned by this goroutine. Capture
		// ownership here, not on the parent goroutine.
		child := ec.childWith(id, name, currentGoroutineOwner(), mode, defaults)
		child.adoptBranchToken(tok)

		opInfo := dispatchContextStart(ec, id, name, subType, op)

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
			update := childUpdate(ec, id, name, subType, OperationActionFail)
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
			failure := options.failure(name, rec)
			dispatchContextEnd(ec, opInfo, "", failure)
			fut.settle(zero, failure)
			return
		}

		serialized, serr := options.serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
		if serr != nil {
			fut.settle(result, newSerdesError(name, serdesDirectionMarshal, serr))
			return
		}
		update := childUpdate(ec, id, name, subType, OperationActionSucceed)
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
		dispatchContextEnd(ec, opInfo, aws.ToString(update.Payload), nil)

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
// [WithChildSummary] to its checkpoint when the result is oversized,
// [WithChildSubType] to its recorded subtype, and [WithChildErrorMapper]
// to its failure.
//
// The child's operation identity is claimed before Go returns, so
// consecutive Go calls from one goroutine are replay-deterministic. Inside
// fn, the provided Context is owned by fn's goroutine, and all durable
// operations on it are safe, including nested Go calls.
func Go[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) *Future[O] {
	return RunInChildContextAsync(ctx, name, fn, opts...)
}

// claimVirtualChild claims the operation ID of the virtual child context
// name on ec, or reports why it cannot run there. A virtual child context
// cannot hold another: neither wrapper would leave a record, so the inner
// one would add only an operation-ID namespace level. The option is
// rejected there, before an ID is claimed, so a virtual level is always
// one deep, as a flat batch item is. The ID is claimed without touching
// ec's replay mode (see claimUncheckpointedOperation): a virtual child
// records nothing, so its absence from the checkpoint log does not mean
// replay has ended. A checkpoint at the claimed position belongs to code
// that ran a checkpointed operation there, so that is a non-deterministic
// replay.
func claimVirtualChild(ec *execContext, name string) (string, error) {
	if ec.virtual {
		return "", fmt.Errorf("durable: child context %q: WithChildVirtual cannot be used inside a virtual child context; make one of the two a checkpointed child context", name)
	}
	id, err := ec.claimUncheckpointedOperation()
	if err != nil {
		return "", err
	}
	if op := ec.state.get(id); op != nil {
		e := &NonDeterministicReplayError{
			Name:          name,
			StepID:        op.id,
			ExpectedName:  name,
			ActualType:    op.opType,
			ActualSubType: op.subType,
			ActualName:    op.name,
		}
		e.detail = fmt.Sprintf(
			"durable: non-deterministic replay at step %q (name %q): "+
				"the checkpoint records a %s/%s operation named %q, but the code declares a virtual child context, "+
				"which records no operation of its own — the handler code changed between deployments",
			op.id, name, op.opType, op.subType, op.name)
		return "", e
	}
	return id, nil
}

// runVirtualChild is [RunInChildContext] for a child context with
// [WithChildVirtual]. It claims the child's operation ID as a checkpointed
// child would, so the operations inside it are numbered the same way, but
// checkpoints nothing for the child itself. It dispatches the same
// operation lifecycle hooks as a checkpointed child, with subType as the
// operation's subtype: a start before [Plugin].WrapChildContextFn wraps
// fn, and an end once fn has an outcome; see dispatchVirtualContextStart
// for the IsReplay each hook reports. fn runs on the calling goroutine in
// the replay mode inherited from ec (see virtualChildReplayMode).
// Suspension propagates unchanged and dispatches no end; any other
// failure of fn is returned in the shape of a first-run failure of a
// checkpointed child, and the result is round-tripped through the serdes.
func runVirtualChild[O any](ec *execContext, name, subType string, options childOptions, fn func(Context) (O, error)) (O, error) {
	var zero O
	id, err := claimVirtualChild(ec, name)
	if err != nil {
		return zero, err
	}
	mode := virtualChildReplayMode(ec)
	child := ec.virtualChildContextWith(id, name, ec.owner, mode, ec.inheritedDefaults())

	opInfo := dispatchVirtualContextStart(ec, id, name, subType)

	var fnTrace []string
	wrappedResult, wrappedErr := wrapVirtualChildBody(ec, opInfo, mode, func(ctx context.Context) (any, error) {
		child.Context = ctx
		r, e := fn(child)
		if e != nil {
			fnTrace = ec.returnedErrorTrace(fn, e, 0)
		}
		return r, e
	})
	if wrappedErr != nil {
		return zero, virtualChildFailure(ec, opInfo, name, options, wrappedErr, fnTrace)
	}
	var result O
	if wrappedResult != nil {
		result, _ = wrappedResult.(O)
	}
	return virtualChildSuccess(ec, opInfo, name, options, result)
}

// wrapVirtualChildBody runs body, the body of the virtual child context
// start describes, through every plugin's [Plugin].WrapChildContextFn,
// as the checkpointed child paths do. The hooks receive start's info with
// IsReplay set from mode, the mode the child runs in: false when the child
// executes live, true when it replays the operations inside it. The
// context the innermost hook supplies is passed to body.
func wrapVirtualChildBody(ec *execContext, start OperationHookInfo, mode executionMode, body wrapBody) (any, error) {
	wrapInfo := start
	wrapInfo.IsReplay = mode != modeExecution
	return wrapChain(ec.operationHooks(), ec,
		func(p *Plugin) wrapHook {
			if p.WrapChildContextFn == nil {
				return nil
			}
			return func(ctx context.Context, innerFn wrapBody) (any, error) {
				return p.WrapChildContextFn(ctx, wrapInfo, innerFn)
			}
		},
		body,
	)
}

// runVirtualChildAsync is [RunInChildContextAsync] for a child context with
// [WithChildVirtual]; see runVirtualChild. The child's operation ID is
// claimed synchronously, as for a checkpointed child, so that the IDs of
// concurrent children stay in program order. fn runs on its own goroutine,
// which owns the child context, and the future settles with its outcome.
// The lifecycle hooks are dispatched from that goroutine, as they are for
// a checkpointed asynchronous child, and [Plugin].WrapChildContextFn wraps
// fn there, after the start. Nothing is checkpointed for the child itself,
// so the goroutine holds no executing span: a pending operation inside it
// settles the branch as it would under a checkpointed child.
func runVirtualChildAsync[O any](ec *execContext, name, subType string, options childOptions, fn func(Context) (O, error)) *Future[O] {
	id, err := claimVirtualChild(ec, name)
	if err != nil {
		return newFailedFuture[O](err)
	}
	mode := virtualChildReplayMode(ec)

	fut := newFuture[O]()
	registerFuture(ec.suspend, fut)

	// Snapshot the serializer and logging defaults on the owning goroutine:
	// the owner may call ConfigureSerdes or ConfigureLogging before the
	// goroutine below runs.
	defaults := ec.inheritedDefaults()
	tok := ec.suspend.registerBranchToken()
	go func() {
		defer tok.release()
		child := ec.virtualChildContextWith(id, name, currentGoroutineOwner(), mode, defaults)
		child.adoptBranchToken(tok)

		opInfo := dispatchVirtualContextStart(ec, id, name, subType)

		// Recover panics in the child function and the wrap hooks so
		// they settle the future as a failure rather than crashing the
		// process. The context the hooks supply becomes the child's
		// parent context; the child is not visible to any other
		// goroutine before fn runs.
		result, fnTrace, fnErr := runUserFunc(child, fn, fmt.Sprintf("durable: child context %q panicked", name), func() (O, error) {
			var zero O
			wrappedResult, wrappedErr := wrapVirtualChildBody(ec, opInfo, mode, func(ctx context.Context) (any, error) {
				child.Context = ctx
				return fn(child)
			})
			if wrappedErr != nil {
				return zero, wrappedErr
			}
			if wrappedResult == nil {
				return zero, nil
			}
			r, _ := wrappedResult.(O)
			return r, nil
		})
		if fnErr != nil {
			var zero O
			fut.settle(zero, virtualChildFailure(ec, opInfo, name, options, fnErr, fnTrace))
			return
		}
		fut.settle(virtualChildSuccess(ec, opInfo, name, options, result))
	}()
	return fut
}

// dispatchVirtualContextStart dispatches the start of the virtual child
// context id, whose body is about to run, and returns the dispatched info.
// The context operation hooks of a virtual child mirror those of a
// checkpointed child (see dispatchContextStart): a start before the body
// and before WrapChildContextFn, then an end once the body has an
// outcome, or no end when the body suspends. The IsReplay they report
// differs, because the child records nothing:
//
//   - The start and the end report IsReplay true on every invocation. A
//     checkpointed child reports IsReplay false only on the events that
//     record a checkpoint, and no event of a virtual child records one.
//     The start reports STARTED, as a checkpointed child re-entered before
//     it settles does.
//   - WrapChildContextFn receives the start's info with IsReplay set from
//     the mode the child runs in: false when it executes live, true when
//     it replays the operations inside it. See wrapVirtualChildBody.
//
// The operations inside a virtual child record its parent as theirs, so
// no reported operation names the child as ParentID whatever the plugin
// depth bound; ChildrenOmitted is therefore false: the subtree is absent,
// not omitted.
func dispatchVirtualContextStart(ec *execContext, id, name, subType string) OperationHookInfo {
	info := ec.operationHookInfo(id, name, string(OperationTypeContext), subType, true)
	info.ChildrenOmitted = false
	info.StartTimestamp = time.Now()
	info.Status = PluginOperationStarted
	dispatchOperationStart(ec, info, PluginOperationStarted)
	return info
}

// dispatchVirtualContextEnd dispatches the end of the virtual child
// context start describes, with IsReplay true (see
// dispatchVirtualContextStart). err is the failure the child returns to
// its caller, nil for a child that succeeded with the serialized result.
// The child has no checkpoint to take an end time from, so the end is
// stamped now.
func dispatchVirtualContextEnd(ec *execContext, start OperationHookInfo, result string, err error) {
	info := ec.operationHookInfo(start.ID, start.Name, start.Type, start.SubType, true)
	info.ChildrenOmitted = false
	info.StartTimestamp = start.StartTimestamp
	info.EndTimestamp = time.Now()
	if err != nil {
		info.Error = err
		dispatchOperationEnd(ec, info, PluginOperationFailed)
		return
	}
	info.Result = result
	dispatchOperationEnd(ec, info, PluginOperationSucceeded)
}

// virtualChildFailure builds the error a virtual child context returns
// when its body fails with fnErr, and dispatches the child's end with it.
// Suspension is not a failure and dispatches no end; see
// replayedChildFailure for the shape of the error.
func virtualChildFailure(ec *execContext, start OperationHookInfo, name string, options childOptions, fnErr error, fnTrace []string) error {
	failure := replayedChildFailure(name, options, fnErr, fnTrace)
	if !errors.Is(failure, errSuspendExecution) {
		dispatchVirtualContextEnd(ec, start, "", failure)
	}
	return failure
}

// virtualChildSuccess round-trips result through the child's serdes, keyed
// on the virtual child's operation ID, so the value returned live equals
// the value returned on replay, which re-runs the body, and dispatches the
// child's end with the serialized result. A virtual child has no
// checkpoint that could settle it, so its outcome is whatever this
// function returns to the caller: a serdes failure in either direction
// dispatches a failed end carrying the [SerdesError] the caller receives,
// so every start of a virtual child is followed by exactly one end.
func virtualChildSuccess[O any](ec *execContext, start OperationHookInfo, name string, options childOptions, result O) (O, error) {
	var zero O
	serialized, err := options.serdes.Marshal(ec.Context, ec.serdesCtx(start.ID), result)
	if err != nil {
		failure := newSerdesError(name, serdesDirectionMarshal, err)
		dispatchVirtualContextEnd(ec, start, "", failure)
		return zero, failure
	}
	var out O
	if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(start.ID), serialized, &out); err != nil {
		failure := newSerdesError(name, serdesDirectionUnmarshal, err)
		dispatchVirtualContextEnd(ec, start, "", failure)
		return zero, failure
	}
	dispatchVirtualContextEnd(ec, start, string(serialized), nil)
	return out, nil
}

// resolveTerminalChild handles a child operation that already has a terminal
// status in the checkpoint log. It returns a pre-settled future, except in
// ReplayChildren mode, where the child body runs again on its own goroutine
// and the future settles when it finishes. The replayed end is dispatched
// here, before the result is rebuilt, as in [RunInChildContext]. subType is
// the operation's resolved subtype.
func resolveTerminalChild[O any](ec *execContext, op *operation, id, name, subType string, options childOptions, fn func(Context) (O, error)) *Future[O] {
	switch op.status {
	case statusSucceeded:
		if op.childCtx == nil {
			return newFailedFuture[O](fmt.Errorf("durable: child context %q: checkpointed %s operation has no context details", name, op.status))
		}
		dispatchReplayedContextEnd(ec, id, name, subType, op, nil)
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
		failure := options.failure(name, childFailureRecord(op))
		dispatchReplayedContextEnd(ec, id, name, subType, op, failure)
		return newFailedFuture[O](failure)

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

	// Snapshot the serializer and logging defaults on the owning goroutine:
	// the owner may call ConfigureSerdes or ConfigureLogging before the
	// goroutine below runs.
	defaults := ec.inheritedDefaults()
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
// on a run that records nothing for the child itself: re-execution in
// ReplayChildren mode, or any run of a virtual child context. Suspension is
// not a child failure: it propagates as errSuspendExecution so the
// invocation ends PENDING and the child resumes in a later invocation. Any
// other error is wrapped through the configured mapper exactly as a
// first-run failure is.
func replayedChildFailure(name string, options childOptions, fnErr error, fnTrace []string) error {
	if errors.Is(fnErr, errSuspendExecution) || errors.Is(fnErr, errCheckpointTerminated) {
		return errSuspendExecution
	}
	return options.failure(name, recordOf(fnErr).withTrace(fnTrace))
}

// childReplayMode determines the execution mode for a child context. A
// child whose overall result is SUCCEEDED uses modeReplaySucceededContext
// so in-flight operations within it block rather than re-execute. Any
// other child replays when an operation is already checkpointed inside it.
//
// Two probes find such an operation. The first is the child's first
// operation, id+"-1"; it is the only probe a FLAT batch item can use,
// because the operations inside a flat item record the batch, not the
// item, as their parent. The second asks whether any checkpointed
// operation records id as its parent, which covers a checkpointed child
// whose first operation left no checkpoint: a virtual child context
// ([WithChildVirtual]) with nothing durable inside it, followed by a
// checkpointed operation.
func childReplayMode(ec *execContext, id string, op *operation) executionMode {
	if op != nil && op.status == statusSucceeded {
		return modeReplaySucceededContext
	}
	if ec.state.get(id+"-1") != nil || ec.state.hasChildOf(id) {
		return modeReplay
	}
	return modeExecution
}

// virtualChildReplayMode determines the execution mode for a virtual child
// context on ec. The child has no checkpoint of its own, and its
// operations are recorded where ec's own operations are, so it starts in
// ec's current mode. A child that inherits modeReplay switches to live
// execution at its first operation with no checkpoint, as ec itself
// would; one that inherits modeReplaySucceededContext keeps it, so its
// unfinished operations park instead of re-executing.
func virtualChildReplayMode(ec *execContext) executionMode {
	return executionMode(ec.mode.Load())
}

// childUpdate assembles the shared fields of a child-context operation
// update. subType is the operation's resolved subtype. IDs are hashed to
// their wire form.
func childUpdate(ec *execContext, id, name, subType string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeContext,
		SubType: aws.String(subType),
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

// dispatchContextStart dispatches the start of the context operation id,
// which is about to run its body, and returns the dispatched info. op is
// the operation's checkpoint as it was before any START checkpoint: nil
// for a live context, else the unsettled record the context re-enters.
//
// dispatchContextStart, dispatchContextEnd, and dispatchReplayedContextEnd
// are the operation lifecycle hooks of a context operation, shared by
// RunInChildContext, RunInChildContextAsync, and WaitForCallback. A context
// operation dispatches at most one start and at most one end per
// invocation:
//
//   - A context that runs its body dispatches a start first. A live
//     context, one with no checkpoint yet, reports STARTED with IsReplay
//     false after its START checkpoint. A context re-entered while its
//     checkpoint is still unsettled reports the checkpointed status with
//     IsReplay true. Either way the start is dispatched before the body,
//     and before WrapChildContextFn, so a plugin can correlate the two.
//   - The body's outcome is recorded by this invocation, so the end that
//     follows the terminal checkpoint is live: IsReplay false. A body that
//     suspends has no outcome, so no end is dispatched.
//   - A context replayed from a terminal checkpoint dispatches only a
//     replayed end with the checkpointed timestamps and outcome: its start
//     was dispatched by the invocation that recorded it.
//
// The events are dispatched on the parent context ec, so ParentID names
// the context that claimed the operation, and nested contexts report a
// ParentID chain that mirrors their checkpoint hierarchy.
func dispatchContextStart(ec *execContext, id, name, subType string, op *operation) OperationHookInfo {
	info := ec.operationHookInfo(id, name, string(OperationTypeContext), subType, op != nil)
	status := PluginOperationStarted
	if op != nil {
		info.StartTimestamp = op.startTimestamp
		status = toPluginOperationStatus(op.status)
	} else {
		info.StartTimestamp = checkpointedStartTime(ec.state.get(id))
	}
	info.Status = status
	dispatchOperationStart(ec, info, status)
	return info
}

// dispatchContextEnd dispatches the live end of the context operation id
// once its terminal checkpoint is recorded. start is the info returned by
// dispatchContextStart. err is the failure the operation returns to its
// caller, nil for a succeeded context whose checkpointed payload is
// result.
func dispatchContextEnd(ec *execContext, start OperationHookInfo, result string, err error) {
	info := ec.operationHookInfo(start.ID, start.Name, start.Type, start.SubType, false)
	info.ChildrenOmitted = start.ChildrenOmitted
	info.StartTimestamp = start.StartTimestamp
	info.EndTimestamp = checkpointedEndTime(ec.state.get(start.ID))
	if err != nil {
		info.Error = err
		dispatchOperationEnd(ec, info, PluginOperationFailed)
		return
	}
	info.Result = result
	dispatchOperationEnd(ec, info, PluginOperationSucceeded)
}

// dispatchReplayedContextEnd dispatches the replayed end of the context
// operation id whose checkpoint op is terminal. err is the failure the
// operation returns to its caller, nil for a SUCCEEDED context, whose
// checkpointed payload is reported as Result.
func dispatchReplayedContextEnd(ec *execContext, id, name, subType string, op *operation, err error) {
	info := ec.operationHookInfo(id, name, string(OperationTypeContext), subType, true)
	info.StartTimestamp = op.startTimestamp
	info.EndTimestamp = op.endTimestamp
	if err != nil {
		info.Error = err
	} else if op.childCtx != nil {
		info.Result = op.childCtx.result
	}
	dispatchOperationEnd(ec, info, toPluginOperationStatus(op.status))
}

// checkpointedEndTime returns op's end timestamp when op exists and
// carries one, else the current time. Used for the end hook of a live
// operation right after its terminal checkpoint: the checkpoint response
// may or may not carry the operation record with its timestamp.
func checkpointedEndTime(op *operation) time.Time {
	if op != nil && !op.endTimestamp.IsZero() {
		return op.endTimestamp
	}
	return time.Now()
}
