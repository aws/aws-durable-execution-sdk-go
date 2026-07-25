package durable

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// operationSubTypeCallback is the wire subtype for callback operations.
const operationSubTypeCallback = "Callback"

// operationSubTypeWaitForCallback is the wire subtype for the
// WaitForCallback child-context wrapper.
const operationSubTypeWaitForCallback = "WaitForCallback"

// Callback is a pending callback operation. It carries the identifier that
// an external system uses to submit a result, and it settles when the
// submission arrives or the timeout elapses.
type Callback[O any] struct {
	id     string
	future *Future[O]
}

// ID returns the callback identifier to hand to the external system.
func (c *Callback[O]) ID() string {
	return c.id
}

// Done returns a channel that is closed when the callback settles.
func (c *Callback[O]) Done() <-chan struct{} {
	return c.future.Done()
}

// Result blocks until the external system submits a result or the timeout
// elapses, then returns the outcome. Failures are returned as a
// [*CallbackError].
func (c *Callback[O]) Result() (O, error) {
	return c.future.Result()
}

// CreateCallback creates a callback that an external system completes with
// the SendDurableExecutionCallbackSuccess or
// SendDurableExecutionCallbackFailure APIs. The returned callback exposes
// the identifier to hand off and the settled result.
func CreateCallback[O any](ctx Context, name string, opts ...CallbackOption) (*Callback[O], error) {
	ec, ok := ctx.(*execContext)
	if !ok {
		return nil, fmt.Errorf("durable: CreateCallback %q: Context was not created by the SDK", name)
	}

	options := callbackOptions{}
	for _, o := range opts {
		o.applyCallback(&options)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return nil, err
	}

	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeCallback), operationSubTypeCallback, name); err != nil {
		return nil, err
	}
	if op != nil {
		serdes := callbackDeserializerForOptions(ec, options)
		switch op.status {
		case statusSucceeded:
			cb := resolveCallbackSuccess[O](op, id, name, serdes, ec.serdesCtx(id))
			return cb, nil

		case statusFailed:
			cb := resolveCallbackFailure[O](op, name)
			return cb, nil

		case statusTimedOut:
			cb := resolveCallbackTimeout[O](op, name)
			return cb, nil

		case statusStarted, statusPending:
			// Callback is in flight; create a future that fires
			// suspend when Result() is called (deferred suspension).
			callbackID := ""
			if op.callback != nil {
				callbackID = op.callback.callbackID
			}
			fut := newPendingCallbackFuture[O](ec.suspend)
			return &Callback[O]{id: callbackID, future: fut}, nil

		default:
			return nil, fmt.Errorf("durable: callback %q: unexpected checkpointed status %s", name, op.status)
		}
	}

	// First invocation: checkpoint START.
	update := callbackUpdate(ec, id, name, types.OperationActionStart)
	update.CallbackOptions = buildCallbackOptions(options)
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return nil, err
	}

	// After checkpointing START, the backend assigns a CallbackId and
	// returns it in the checkpoint response (merged into state). Read it
	// so the submitter step (in WaitForCallback) can use it.
	callbackID := ""
	if updated := ec.state.get(id); updated != nil && updated.callback != nil {
		callbackID = updated.callback.callbackID
	}

	// Return a callback whose Result() fires suspend on first call.
	// This allows WaitForCallback to run the submitter step between
	// CreateCallback and cb.Result() in the same invocation.
	fut := newPendingCallbackFuture[O](ec.suspend)

	return &Callback[O]{id: callbackID, future: fut}, nil
}

// WaitForCallback creates a callback, runs submitter to deliver the
// callback identifier to an external system, and blocks until the system
// submits a result or the timeout elapses. Failures are returned as a
// [*CallbackError].
//
// Internally WaitForCallback wraps a child context containing a callback
// and a submitter step, matching the WaitForCallback wire shape used by
// all SDK implementations.
func WaitForCallback[O any](ctx Context, name string, submitter func(ctx StepContext, callbackID string) error, opts ...CallbackOption) (O, error) {
	var zero O
	ec, ok := ctx.(*execContext)
	if !ok {
		return zero, fmt.Errorf("durable: WaitForCallback %q: Context was not created by the SDK", name)
	}

	options := callbackOptions{}
	for _, o := range opts {
		o.applyCallback(&options)
	}

	// WaitForCallback is a child context (SubType WaitForCallback) that:
	// 1. Creates an inner callback (no name)
	// 2. Runs the submitter as a step passing the callbackID
	// 3. Returns the callback result
	//
	// We use the RunInChildContext machinery but with our own subtype.
	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}

	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(types.OperationTypeContext), operationSubTypeWaitForCallback, name); err != nil {
		return zero, err
	}

	// Terminal states: the whole WaitForCallback context is settled.
	if op != nil {
		switch op.status {
		case statusSucceeded:
			if op.childCtx == nil {
				return zero, fmt.Errorf("durable: WaitForCallback %q: checkpointed SUCCEEDED has no context details", name)
			}
			var out O
			if err := ec.serdes.Unmarshal(ec.serdesCtx(id), []byte(op.childCtx.result), &out); err != nil {
				return zero, fmt.Errorf("durable: WaitForCallback %q: deserialize result: %w", name, err)
			}
			return out, nil

		case statusFailed:
			return zero, wfcbFailedError(op, name)

		case statusStarted, statusPending, statusReady:
			// Context is in flight; fall through to execute/replay.
		default:
			return zero, fmt.Errorf("durable: WaitForCallback %q: unexpected status %s", name, op.status)
		}
	}

	// Checkpoint ContextStarted (SubType WaitForCallback) if first time.
	if op == nil {
		update := wfcbContextUpdate(ec, id, name, types.OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
			return zero, err
		}
	}

	// Run the inner child: callback + submitter step.
	mode := childReplayMode(ec, id, op)
	child := ec.child(id, ec.owner, mode)

	result, fnErr := runWaitForCallbackBody[O](child, name, submitter, options, ec.serdes)
	if fnErr != nil {
		if errors.Is(fnErr, errSuspendExecution) {
			return zero, fnErr
		}
		// Checkpoint ContextFailed.
		update := wfcbContextUpdate(ec, id, name, types.OperationActionFail)
		update.Error = errorObject(fnErr)
		if cerr := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); cerr != nil {
			return zero, cerr
		}
		return zero, fnErr
	}

	// Checkpoint ContextSucceeded.
	serialized, serr := ec.serdes.Marshal(ec.serdesCtx(id), result)
	if serr != nil {
		return zero, fmt.Errorf("durable: WaitForCallback %q: serialize result: %w", name, serr)
	}
	update := wfcbContextUpdate(ec, id, name, types.OperationActionSucceed)
	update.Payload = aws.String(string(serialized))
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return zero, err
	}

	// Round-trip for consistency (first-run == replay).
	var out O
	if err := ec.serdes.Unmarshal(ec.serdesCtx(id), serialized, &out); err != nil {
		return zero, fmt.Errorf("durable: WaitForCallback %q: deserialize result: %w", name, err)
	}
	return out, nil
}

// runWaitForCallbackBody is the inner function of WaitForCallback: create
// callback + run submitter step + return callback result.
func runWaitForCallbackBody[O any](child *execContext, name string, submitter func(StepContext, string) error, options callbackOptions, serdes Serdes) (O, error) {
	var zero O

	// Step 1: create the inner callback (unnamed, per wire spec).
	cb, err := CreateCallback[O](child, "", WithCallbackTimeout(options.timeout), WithCallbackHeartbeatTimeout(options.heartbeatTimeout))
	if err != nil {
		return zero, err
	}

	// Step 2: run the submitter as a step (uses default retry unless
	// overridden by options.retryStrategy).
	callbackID := cb.ID()
	var stepOpts []StepOption
	if options.retryStrategy != nil {
		stepOpts = append(stepOpts, WithRetry(options.retryStrategy))
	}
	_, stepErr := Step[Void](child, "", func(sc StepContext) (Void, error) {
		return Void{}, submitter(sc, callbackID)
	}, stepOpts...)
	if stepErr != nil {
		return zero, stepErr
	}

	// Step 3: await the callback result.
	result, cbErr := cb.Result()
	if cbErr != nil {
		return zero, cbErr
	}
	return result, nil
}

// dagCallbackContainer runs the native WaitForCallback operation inside an
// outer container context checkpointed with SubType "Callback". A DAG
// callback task materializes as a Callback container (whose Id/Name identify
// the DAG task node) whose body is the native WaitForCallback operation,
// matching the cross-language wire shape: ContextStarted(Callback) ->
// ContextStarted(WaitForCallback) -> ... -> ContextSucceeded(WaitForCallback)
// -> ContextSucceeded(Callback). This wrapper is DAG-specific; a standalone
// WaitForCallback emits only the WaitForCallback context.
//
// The container reuses the DAG's own CONTEXT-op recipe (dagMaterializeChild /
// dagFinishChild), so like the DAG scope it is checkpointed with
// ReplayChildren: the body re-runs on replay and the inner WaitForCallback
// fast-paths from its own checkpoint, returning the correctly typed result.
func dagCallbackContainer[O any](ctx Context, name string, submitter func(StepContext, string) error, opts ...CallbackOption) (O, error) {
	var zero O
	ec, ok := ctx.(*execContext)
	if !ok {
		return zero, fmt.Errorf("durable: dag callback %q: Context was not created by the SDK", name)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}

	child, terminal, op, err := dagMaterializeChild(ec, id, name, operationSubTypeCallback)
	if err != nil {
		return zero, err
	}
	if terminal && op != nil && op.status == statusFailed {
		cause := &replayedError{errType: "Error", message: "dag callback failed"}
		if op.childCtx != nil {
			cause = &replayedError{errType: op.childCtx.errType, message: op.childCtx.errMessage}
		}
		return zero, &ChildContextError{Name: name, Err: cause}
	}

	result, fnErr := WaitForCallback[O](child, name, submitter, opts...)
	if fnErr != nil {
		// Suspension is not a failure: propagate it unchanged so the
		// invocation ends PENDING and the callback resumes later.
		if errors.Is(fnErr, errSuspendExecution) {
			return zero, fnErr
		}
		if !terminal {
			if cerr := dagFinishChild(ec, id, name, operationSubTypeCallback, fnErr); cerr != nil {
				return zero, cerr
			}
		}
		return zero, fnErr
	}

	if !terminal {
		if err := dagFinishChild(ec, id, name, operationSubTypeCallback, nil); err != nil {
			return zero, err
		}
	}
	return result, nil
}

// wfcbFailedError constructs the appropriate error when a WaitForCallback
// context has a FAILED checkpointed status.
func wfcbFailedError(op *operation, name string) error {
	cause := &replayedError{errType: "Error", message: "wait-for-callback failed"}
	if op.childCtx != nil {
		cause = &replayedError{errType: op.childCtx.errType, message: op.childCtx.errMessage}
	}
	// Propagate callback errors directly (they bubble through the
	// child-context failure wrapping on the wire).
	if cause.errType == "CallbackError" || cause.errType == "Callback.Timeout" || cause.errType == "Callback.Heartbeat" {
		// Set sentinel so errors.Is traverses the Unwrap chain correctly,
		// matching the pattern in invokeErrorFromCheckpoint.
		if cause.errType == "Callback.Timeout" {
			cause.sentinel = ErrCallbackTimedOut
		}
		return &CallbackError{Name: name, Err: cause}
	}
	return &ChildContextError{Name: name, Err: cause}
}

// resolveCallbackSuccess creates a pre-settled callback for a SUCCEEDED
// checkpointed status.
func resolveCallbackSuccess[O any](op *operation, id, name string, serdes Serdes, sctx SerdesContext) *Callback[O] {
	if op.callback == nil {
		fut := newFailedFuture[O](fmt.Errorf("durable: callback %q: SUCCEEDED but no callback details", name))
		return &Callback[O]{id: "", future: fut}
	}
	// Empty result payload means the external system sent success with no
	// payload — return the zero value of the type.
	if op.callback.result == "" {
		var zero O
		return &Callback[O]{id: op.callback.callbackID, future: newSettledFuture(zero, nil)}
	}
	var out O
	if err := serdes.Unmarshal(sctx, []byte(op.callback.result), &out); err != nil {
		fut := newFailedFuture[O](fmt.Errorf("durable: callback %q: deserialize result: %w", name, err))
		return &Callback[O]{id: op.callback.callbackID, future: fut}
	}
	return &Callback[O]{id: op.callback.callbackID, future: newSettledFuture(out, nil)}
}

// resolveCallbackFailure creates a pre-settled callback for a FAILED
// checkpointed status.
func resolveCallbackFailure[O any](op *operation, name string) *Callback[O] {
	callbackID := ""
	cause := &replayedError{errType: "Error", message: "callback failed"}
	if op.callback != nil {
		callbackID = op.callback.callbackID
		cause = &replayedError{errType: op.callback.errType, message: op.callback.errMessage}
	}
	cbErr := &CallbackError{Name: name, CallbackID: callbackID, Err: cause}
	return &Callback[O]{id: callbackID, future: newFailedFuture[O](cbErr)}
}

// resolveCallbackTimeout creates a pre-settled callback for a TIMED_OUT
// checkpointed status.
func resolveCallbackTimeout[O any](op *operation, name string) *Callback[O] {
	callbackID := ""
	if op.callback != nil {
		callbackID = op.callback.callbackID
	}
	cbErr := &CallbackError{Name: name, CallbackID: callbackID, Err: ErrCallbackTimedOut}
	return &Callback[O]{id: callbackID, future: newFailedFuture[O](cbErr)}
}

// callbackUpdate assembles the shared fields of a callback operation update.
func callbackUpdate(ec *execContext, id, name string, action types.OperationAction) types.OperationUpdate {
	update := types.OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    types.OperationTypeCallback,
		SubType: aws.String(operationSubTypeCallback),
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

// wfcbContextUpdate assembles a WaitForCallback context operation update.
func wfcbContextUpdate(ec *execContext, id, name string, action types.OperationAction) types.OperationUpdate {
	update := types.OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    types.OperationTypeContext,
		SubType: aws.String(operationSubTypeWaitForCallback),
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

// buildCallbackOptions produces the wire CallbackOptions from the SDK
// options. Returns nil when neither timeout is set.
func buildCallbackOptions(opts callbackOptions) *types.CallbackOptions {
	timeout := int32(0)
	heartbeat := int32(0)
	if opts.timeout > 0 {
		timeout = int32(math.Ceil(opts.timeout.Seconds()))
	}
	if opts.heartbeatTimeout > 0 {
		heartbeat = int32(math.Ceil(opts.heartbeatTimeout.Seconds()))
	}
	if timeout == 0 && heartbeat == 0 {
		return nil
	}
	return &types.CallbackOptions{
		TimeoutSeconds:          timeout,
		HeartbeatTimeoutSeconds: heartbeat,
	}
}

// CallbackOption configures a single callback operation.
type CallbackOption interface {
	applyCallback(*callbackOptions)
}

// WithCallbackTimeout bounds how long the callback waits for an external
// submission. On expiry the callback fails with a [*CallbackError] matching
// [ErrCallbackTimedOut].
func WithCallbackTimeout(d time.Duration) CallbackOption {
	return callbackOptionFunc(func(o *callbackOptions) { o.timeout = d })
}

// WithCallbackHeartbeatTimeout bounds the interval between heartbeats from
// the external system. If no heartbeat arrives within the interval, the
// callback fails with a [*CallbackError] matching [ErrCallbackTimedOut].
func WithCallbackHeartbeatTimeout(d time.Duration) CallbackOption {
	return callbackOptionFunc(func(o *callbackOptions) { o.heartbeatTimeout = d })
}

// WithSubmitterRetry configures a retry strategy for the submitter step in
// [WaitForCallback]. The submitter function re-executes on failure
// according to this strategy.
func WithSubmitterRetry(s RetryStrategy) CallbackOption {
	return callbackOptionFunc(func(o *callbackOptions) { o.retryStrategy = s })
}

// WithCallbackSerdes overrides the serializer for the callback result. The
// deserialize path is used when replaying a SUCCEEDED callback to unmarshal
// the stored payload into the typed result.
func WithCallbackSerdes(s Serdes) CallbackOption {
	return callbackOptionFunc(func(o *callbackOptions) { o.serdes = s })
}

type callbackOptions struct {
	timeout          time.Duration
	heartbeatTimeout time.Duration
	retryStrategy    RetryStrategy
	serdes           Serdes
}

type callbackOptionFunc func(*callbackOptions)

func (f callbackOptionFunc) applyCallback(o *callbackOptions) { f(o) }

// callbackDeserializerForOptions returns the effective Serdes for
// deserializing a callback result, respecting the precedence:
// per-op WithCallbackSerdes > handler-level WithCallbackDeserializer > default serdes.
func callbackDeserializerForOptions(ec *execContext, opts callbackOptions) Serdes {
	if opts.serdes != nil {
		return opts.serdes
	}
	if ec.callbackDeserializer != nil {
		return deserializerSerdes{d: ec.callbackDeserializer}
	}
	return ec.serdes
}

// deserializerSerdes adapts a [Deserializer] to the [Serdes] interface.
// Marshal delegates to the execution context's standard serdes since the
// handler-level callback deserializer only affects the unmarshal path.
type deserializerSerdes struct {
	d Deserializer
}

func (s deserializerSerdes) Marshal(ctx SerdesContext, v any) ([]byte, error) {
	return jsonSerdes{}.Marshal(ctx, v)
}

func (s deserializerSerdes) Unmarshal(_ SerdesContext, data []byte, v any) error {
	return s.d.Unmarshal(data, v)
}
