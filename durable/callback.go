package durable

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// operationSubTypeCallback is the wire subtype for callback operations.
const operationSubTypeCallback = "Callback"

// operationSubTypeWaitForCallback is the wire subtype for the
// WaitForCallback child-context wrapper.
const operationSubTypeWaitForCallback = "WaitForCallback"

// Callback is a pending callback operation. It carries the identifier that
// an external system uses to submit a result, and it settles when the
// submission arrives or the timeout elapses.
//
// The only way to wait on a Callback is [Callback.Result]. Like [Future],
// a Callback exposes no channel, because a select over settle signals is
// not replay-safe.
type Callback[O any] struct {
	id     string
	future *Future[O]
}

// ID returns the callback identifier to hand to the external system.
func (c *Callback[O]) ID() string {
	return c.id
}

// Result blocks until the external system submits a result or the timeout
// elapses, then returns the outcome. Failures are returned as a
// [*CallbackExternalError] or a [*CallbackTimeoutError]; both match
// [*CallbackError].
func (c *Callback[O]) Result() (O, error) {
	return c.future.Result()
}

// CreateCallback creates a callback that an external system completes with
// the SendDurableExecutionCallbackSuccess or
// SendDurableExecutionCallbackFailure APIs. The returned callback exposes
// the identifier to hand off and the settled result.
//
// The submitted payload is deserialized into O with, in order of
// precedence, the per-operation [WithCallbackSerdes], the handler-level
// [WithCallbackDeserializer], or the handler-level [Serdes] set with
// [WithSerdes], which defaults to encoding/json. So a payload of "42"
// deserializes into an int and a payload of "\"ok\"" into a string. This
// differs from the other Durable Execution SDKs, whose callbacks default to
// returning the raw payload string; in Go the result is typed, so it goes
// through the same JSON decoding as every other operation result.
//
// CreateCallback accepts only [CallbackOption] values. Options that
// configure the submitter step of [WaitForCallback], such as
// [WithSubmitterRetry], have no effect here and do not compile.
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
	if err := validateReplayConsistency(op, string(OperationTypeCallback), operationSubTypeCallback, name); err != nil {
		return nil, err
	}
	if ec.unfinishedInSucceededContext(op) {
		// The callback was still unresolved when the enclosing context's
		// result was recorded. Do not checkpoint and do not commit the
		// invocation to PENDING: return a callback whose future settles
		// only if the invocation suspends.
		callbackID := ""
		if op != nil && op.callback != nil {
			callbackID = op.callback.callbackID
		}
		return &Callback[O]{id: callbackID, future: newUnfinishedReplayFuture[O](ec.suspend)}, nil
	}
	if op != nil {
		serdes := callbackDeserializerForOptions(ec, options)
		switch op.status {
		case statusSucceeded:
			cb := resolveCallbackSuccess[O](ec.Context, op, id, name, serdes, ec.serdesCtx(id))
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
			fut := newPendingCallbackFuture[O](ec.suspend, ec)
			return &Callback[O]{id: callbackID, future: fut}, nil

		default:
			return nil, fmt.Errorf("durable: callback %q: unexpected checkpointed status %s", name, op.status)
		}
	}

	// First invocation: checkpoint START.
	update := callbackUpdate(ec, id, name, OperationActionStart)
	cbOpts, cbErr := buildCallbackOptions(options)
	if cbErr != nil {
		return nil, fmt.Errorf("durable: CreateCallback %q: %w", name, cbErr)
	}
	update.CallbackOptions = cbOpts
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
		if errors.Is(err, errCheckpointTerminated) {
			return nil, errSuspendExecution
		}
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
	fut := newPendingCallbackFuture[O](ec.suspend, ec)

	return &Callback[O]{id: callbackID, future: fut}, nil
}

// WaitForCallback creates a callback, runs submitter to deliver the
// callback identifier to an external system, and blocks until the system
// submits a result or the timeout elapses. Failures are returned as a
// [*CallbackExternalError], a [*CallbackTimeoutError], or, when the
// submitter step fails, a [*CallbackSubmitterError]; all match
// [*CallbackError].
//
// Internally WaitForCallback wraps a child context containing a callback
// and a submitter step, matching the WaitForCallback wire shape used by
// all SDK implementations.
//
// WaitForCallback accepts every [CallbackOption], which it applies to the
// callback it creates, plus [WaitForCallbackOption] values such as
// [WithSubmitterRetry] that configure the submitter step.
func WaitForCallback[O any](ctx Context, name string, submitter func(ctx StepContext, callbackID string) error, opts ...WaitForCallbackOption) (O, error) {
	var zero O
	ec, ok := ctx.(*execContext)
	if !ok {
		return zero, fmt.Errorf("durable: WaitForCallback %q: Context was not created by the SDK", name)
	}

	options := callbackOptions{}
	for _, o := range opts {
		o.applyWaitForCallback(&options)
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
	if err := validateReplayConsistency(op, string(OperationTypeContext), operationSubTypeWaitForCallback, name); err != nil {
		return zero, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return zero, ec.parkUnfinishedReplay()
	}

	// Terminal states: the whole WaitForCallback context is settled.
	if op != nil {
		switch op.status {
		case statusSucceeded:
			if op.childCtx == nil {
				return zero, fmt.Errorf("durable: WaitForCallback %q: checkpointed SUCCEEDED has no context details", name)
			}
			var out O
			if err := ec.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.childCtx.result), &out); err != nil {
				return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
			}
			return out, nil

		case statusFailed:
			return zero, wfcbFailedError(ec, op, id, name)

		case statusStarted, statusPending, statusReady:
			// Context is in flight; fall through to execute/replay.
		default:
			return zero, fmt.Errorf("durable: WaitForCallback %q: unexpected status %s", name, op.status)
		}
	}

	// Checkpoint ContextStarted (SubType WaitForCallback) if first time.
	if op == nil {
		update := wfcbContextUpdate(ec, id, name, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			if errors.Is(err, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
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
		fnErr = wfcbMapError(name, fnErr)
		// Checkpoint ContextFailed. The record names the callback failure
		// mode as its ErrorType and carries the cause's message, matching
		// the shape the other SDKs write.
		update := wfcbContextUpdate(ec, id, name, OperationActionFail)
		update.Error = errorObject(fnErr)
		if cbErr, ok := fnErr.(interface{ operationError() *OperationError }); ok {
			update.Error.ErrorMessage = aws.String(cbErr.operationError().Message)
		}
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			if errors.Is(cerr, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
			return zero, cerr
		}
		return zero, fnErr
	}

	// Checkpoint ContextSucceeded.
	serialized, serr := ec.serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
	if serr != nil {
		return zero, newSerdesError(name, serdesDirectionMarshal, serr)
	}
	update := wfcbContextUpdate(ec, id, name, OperationActionSucceed)
	update.Payload = aws.String(string(serialized))
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
		if errors.Is(err, errCheckpointTerminated) {
			return zero, errSuspendExecution
		}
		return zero, err
	}

	// Round-trip for consistency (first-run == replay).
	var out O
	if err := ec.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), serialized, &out); err != nil {
		return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
	}
	return out, nil
}

// runWaitForCallbackBody is the inner function of WaitForCallback: create
// callback + run submitter step + return callback result.
func runWaitForCallbackBody[O any](child *execContext, name string, submitter func(StepContext, string) error, options callbackOptions, serdes Serdes) (O, error) {
	var zero O

	// Step 1: create the inner callback (unnamed, per wire spec). Every
	// callback-level option the caller passed applies to it; the
	// submitter retry strategy is consumed by the step below.
	cbOpts := []CallbackOption{
		WithCallbackTimeout(options.timeout),
		WithCallbackHeartbeatTimeout(options.heartbeatTimeout),
	}
	if options.serdes != nil {
		cbOpts = append(cbOpts, WithCallbackSerdes(options.serdes))
	}
	cb, err := CreateCallback[O](child, "", cbOpts...)
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

// wfcbMapError maps the error that escaped the WaitForCallback body to the
// error WaitForCallback returns. A callback failure passes through with the
// WaitForCallback's name. A failed submitter step becomes a
// [CallbackSubmitterError] carrying the step's final error. Any other
// error is returned unchanged.
//
// This mapping is not expressed as a [WithChildErrorMapper] mapper. A
// mapper receives a [*ChildContextError], which carries only the failure
// record: the wire ErrorType, Message, ErrorData, and StackTrace. The
// mapping here needs more than the record. On the first invocation it keeps
// the live callback error's CallbackID and Heartbeat fields, which the
// record does not carry. On replay, [wfcbFailedError] rebuilds the same
// error from the inner callback and submitter step operations, so a
// callback timeout is distinguished from an external failure by the
// callback operation's status rather than by the context record, and the
// CallbackID is restored. WaitForCallback also checkpoints under its own
// subtype through [wfcbContextUpdate] rather than through
// [RunInChildContext], so there is no child option to attach a mapper to,
// and its context record names the mapped failure mode, the wire shape the
// other SDKs write, where [RunInChildContext] records the mapper's input.
func wfcbMapError(name string, err error) error {
	switch e := err.(type) {
	case *CallbackTimeoutError:
		e.Name = name
		return e
	case *CallbackExternalError:
		e.Name = name
		return e
	case *CallbackSubmitterError:
		e.Name = name
		return e
	case *CallbackError:
		e.Name = name
		return e
	case *StepError:
		return newCallbackSubmitterError(name, "", errorRecord{
			errType: e.ErrorType, message: e.Message, data: e.ErrorData, stackTrace: e.StackTrace,
		})
	}
	return err
}

// wfcbFailedError reconstructs the error for a WaitForCallback context with
// a FAILED checkpointed status. The inner callback and submitter step are
// checkpointed operations of their own, so their records rebuild the same
// error the first invocation returned: a callback timeout or external
// failure from the callback operation, or a submitter failure from the
// step operation. When neither child record is present, the context's own
// record names the failure mode, in the current or an older wire form.
func wfcbFailedError(ec *execContext, op *operation, id, name string) error {
	if cbOp := ec.state.get(id + "-1"); cbOp != nil && cbOp.callback != nil {
		switch cbOp.status {
		case statusTimedOut:
			return newCallbackTimeoutError(name, cbOp.callback.callbackID, cbOp.callback.record())
		case statusFailed:
			return newCallbackExternalError(name, cbOp.callback.callbackID, cbOp.callback.record())
		}
	}
	if stepOp := ec.state.get(id + "-2"); stepOp != nil && stepOp.status == statusFailed && stepOp.step != nil {
		callbackID := ""
		if cbOp := ec.state.get(id + "-1"); cbOp != nil && cbOp.callback != nil {
			callbackID = cbOp.callback.callbackID
		}
		return newCallbackSubmitterError(name, callbackID, stepOp.step.record())
	}
	rec := errorRecord{errType: "Error", message: "wait-for-callback failed"}
	if op.childCtx != nil {
		rec = op.childCtx.record()
	}
	switch {
	case isCallbackTimeoutType(rec.errType):
		return newCallbackTimeoutError(name, "", rec)
	case rec.errType == "CallbackExternalError", rec.errType == "CallbackError":
		return newCallbackExternalError(name, "", rec)
	case rec.errType == "CallbackSubmitterError":
		return newCallbackSubmitterError(name, "", rec)
	}
	return newChildContextError(name, rec)
}

// resolveCallbackSuccess creates a pre-settled callback for a SUCCEEDED
// checkpointed status.
func resolveCallbackSuccess[O any](ctx context.Context, op *operation, id, name string, serdes Serdes, sctx SerdesContext) *Callback[O] {
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
	if err := serdes.Unmarshal(ctx, sctx, []byte(op.callback.result), &out); err != nil {
		fut := newFailedFuture[O](newSerdesError(name, serdesDirectionUnmarshal, err))
		return &Callback[O]{id: op.callback.callbackID, future: fut}
	}
	return &Callback[O]{id: op.callback.callbackID, future: newSettledFuture(out, nil)}
}

// resolveCallbackFailure creates a pre-settled callback for a FAILED
// checkpointed status: the external system reported the failure.
func resolveCallbackFailure[O any](op *operation, name string) *Callback[O] {
	callbackID := ""
	rec := errorRecord{}
	if op.callback != nil {
		callbackID = op.callback.callbackID
		rec = op.callback.record()
	}
	cbErr := newCallbackExternalError(name, callbackID, rec)
	return &Callback[O]{id: callbackID, future: newFailedFuture[O](cbErr)}
}

// resolveCallbackTimeout creates a pre-settled callback for a TIMED_OUT
// checkpointed status.
func resolveCallbackTimeout[O any](op *operation, name string) *Callback[O] {
	callbackID := ""
	rec := errorRecord{}
	if op.callback != nil {
		callbackID = op.callback.callbackID
		rec = op.callback.record()
	}
	cbErr := newCallbackTimeoutError(name, callbackID, rec)
	return &Callback[O]{id: callbackID, future: newFailedFuture[O](cbErr)}
}

// callbackUpdate assembles the shared fields of a callback operation update.
func callbackUpdate(ec *execContext, id, name string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeCallback,
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
func wfcbContextUpdate(ec *execContext, id, name string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeContext,
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
// options. Returns a non-nil error if a timeout duration is negative or
// exceeds the int32 seconds limit. Returns (nil, nil) when neither timeout
// is set.
func buildCallbackOptions(opts callbackOptions) (*CallbackOptions, error) {
	timeout := int32(0)
	heartbeat := int32(0)
	if opts.timeout > 0 {
		sec, err := durationToSeconds(opts.timeout)
		if err != nil {
			return nil, fmt.Errorf("timeout: %w", err)
		}
		timeout = sec
	} else if opts.timeout < 0 {
		_, err := durationToSeconds(opts.timeout)
		return nil, fmt.Errorf("timeout: %w", err)
	}
	if opts.heartbeatTimeout > 0 {
		sec, err := durationToSeconds(opts.heartbeatTimeout)
		if err != nil {
			return nil, fmt.Errorf("heartbeat timeout: %w", err)
		}
		heartbeat = sec
	} else if opts.heartbeatTimeout < 0 {
		_, err := durationToSeconds(opts.heartbeatTimeout)
		return nil, fmt.Errorf("heartbeat timeout: %w", err)
	}
	if timeout == 0 && heartbeat == 0 {
		return nil, nil
	}
	return &CallbackOptions{
		TimeoutSeconds:          timeout,
		HeartbeatTimeoutSeconds: heartbeat,
	}, nil
}

// CallbackOption configures the callback created by [CreateCallback] or
// [WaitForCallback]. Every CallbackOption is also a [WaitForCallbackOption],
// so the same value can be passed to either function.
//
// Options that configure only the submitter step of WaitForCallback, such as
// [WithSubmitterRetry], are not CallbackOptions. Passing one to
// CreateCallback is a compile error rather than a silently ignored option.
type CallbackOption interface {
	WaitForCallbackOption
	applyCallback(*callbackOptions)
}

// WaitForCallbackOption configures a [WaitForCallback] operation. It
// accepts every [CallbackOption] plus options that only apply to the
// submitter step, such as [WithSubmitterRetry].
type WaitForCallbackOption interface {
	applyWaitForCallback(*callbackOptions)
}

// WithCallbackTimeout bounds how long the callback waits for an external
// submission. On expiry the callback fails with a [*CallbackTimeoutError]
// matching [ErrCallbackTimedOut].
func WithCallbackTimeout(d time.Duration) CallbackOption {
	return callbackOptionFunc(func(o *callbackOptions) { o.timeout = d })
}

// WithCallbackHeartbeatTimeout bounds the interval between heartbeats from
// the external system. If no heartbeat arrives within the interval, the
// callback fails with a [*CallbackTimeoutError] whose Heartbeat field is
// true, matching [ErrCallbackTimedOut].
func WithCallbackHeartbeatTimeout(d time.Duration) CallbackOption {
	return callbackOptionFunc(func(o *callbackOptions) { o.heartbeatTimeout = d })
}

// WithSubmitterRetry configures a retry strategy for the submitter step in
// [WaitForCallback]. The submitter function re-executes on failure
// according to this strategy.
//
// WithSubmitterRetry is a [WaitForCallbackOption] only. [CreateCallback]
// has no submitter step, so passing this option to it is a compile error.
func WithSubmitterRetry(s RetryStrategy) WaitForCallbackOption {
	return waitForCallbackOptionFunc(func(o *callbackOptions) { o.retryStrategy = s })
}

// WithCallbackSerdes overrides the serializer for the callback result. The
// deserialize path is used when replaying a SUCCEEDED callback to unmarshal
// the stored payload into the typed result. In [WaitForCallback] it applies
// to the callback the operation creates.
func WithCallbackSerdes(s Serdes) CallbackOption {
	return callbackOptionFunc(func(o *callbackOptions) { o.serdes = s })
}

type callbackOptions struct {
	timeout          time.Duration
	heartbeatTimeout time.Duration
	retryStrategy    RetryStrategy
	serdes           Serdes
}

// callbackOptionFunc is an option that applies to both CreateCallback and
// WaitForCallback. It implements CallbackOption, and through that
// WaitForCallbackOption.
type callbackOptionFunc func(*callbackOptions)

func (f callbackOptionFunc) applyCallback(o *callbackOptions)        { f(o) }
func (f callbackOptionFunc) applyWaitForCallback(o *callbackOptions) { f(o) }

// waitForCallbackOptionFunc is an option that applies only to
// WaitForCallback. It implements WaitForCallbackOption and deliberately not
// CallbackOption.
type waitForCallbackOptionFunc func(*callbackOptions)

func (f waitForCallbackOptionFunc) applyWaitForCallback(o *callbackOptions) { f(o) }

// callbackDeserializerForOptions returns the effective Serdes for
// deserializing a callback result, respecting the precedence:
// per-op WithCallbackSerdes > handler-level WithCallbackDeserializer >
// handler-level Serdes (default encoding/json).
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

func (s deserializerSerdes) Marshal(ctx context.Context, meta SerdesContext, v any) ([]byte, error) {
	return jsonSerdes{}.Marshal(ctx, meta, v)
}

func (s deserializerSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	return s.d.Unmarshal(data, v)
}
