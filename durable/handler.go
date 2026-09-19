package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable/internal/wire"
)

// Handler is a durable function handler. It receives the deserialized
// invocation event and a [Context] in place of the standard Lambda context.
type Handler[I, O any] func(ctx Context, event I) (O, error)

// Wrap adapts handler into a raw payload function for callers that compose
// their own Lambda entry point. The returned function implements no
// aws-lambda-go interface, but its signature matches the runtime's raw
// byte handler method, so adapting it to an entry point is a one-method
// wrapper. [Start] performs exactly that wrapping. Most programs should
// use [Start].
//
// Do not pass the returned function to lambda.Start directly: the
// reflective handler path JSON-decodes the payload into the parameter
// type, and encoding/json expects base64 text for []byte, while the
// durable invocation payload is a JSON object. Register it through the
// runtime's raw byte interface instead, as [Start] does.
func Wrap[I, O any](handler Handler[I, O], opts ...HandlerOption) func(context.Context, []byte) ([]byte, error) {
	if handler == nil {
		panic("durable: handler must not be nil")
	}
	options := handlerOptions{}
	for _, o := range opts {
		o.applyHandler(&options)
	}
	if err := validateHandlerOptions(&options); err != nil {
		panic(err.Error())
	}
	if options.logHandler == nil {
		options.logHandler = defaultLogHandler()
	}
	h := &durableHandler[I, O]{handler: handler, options: options}
	return h.Invoke
}

// HandlerOption configures the durable execution handler at construction
// time.
type HandlerOption interface {
	applyHandler(*handlerOptions)
}

// WithLogHandler sets the [slog.Handler] behind [Context.Logger] and
// [StepContext.Logger]. A nil handler selects the default.
//
// The default handler writes one JSON object per record to stderr, Lambda's
// log channel, with these fields: timestamp (ISO 8601 UTC with millisecond
// precision and a Z suffix), level (DEBUG, INFO, WARN, or ERROR), message,
// requestId, executionArn, tenantId (when the invocation has one),
// operationId, operationName, and attempt (inside a step body, condition
// check, or callback submitter), plus any attributes the call site adds.
// An attribute whose value is an error is expanded into errorType and
// errorMessage, and stackTrace when the error carries recorded frames.
// The default handler's minimum level is read from the AWS_LAMBDA_LOG_LEVEL
// environment variable (TRACE, DEBUG, INFO, WARN, ERROR, or FATAL, case
// insensitive); unset or unrecognised selects INFO. A supplied handler
// applies its own level.
//
// The SDK adds the execution and operation attributes through the
// handler's WithAttrs method, so a supplied handler receives them as
// structured attributes, and it wraps the handler with per-branch replay
// suppression: while a context replays checkpointed operations, its
// records are dropped before they reach the handler, unless
// [WithReplayLogMode] selects [ReplayLogModeEmit]. Fields a plugin
// returns from [Plugin.EnrichLogContext] arrive as attributes of the
// record; that field documents their precedence. To replace the handler
// from inside the handler body, see [ConfigureLogging].
func WithLogHandler(h slog.Handler) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.logHandler = h })
}

// WithReplayLogMode sets what happens to log records emitted while a
// context replays checkpointed operations. The default,
// [ReplayLogModeSuppress], drops them so replayed code does not duplicate
// the lines it wrote when it first ran. [ReplayLogModeEmit] emits them
// with the attribute replay=true, for diagnosing a replay problem; expect
// every line written before a suspension to appear again on each later
// invocation. [ReplayLogModeUnchanged] selects the default. To change the
// mode from inside the handler body, see [ConfigureLogging].
func WithReplayLogMode(mode ReplayLogMode) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.replayLogMode = mode })
}

// WithSerdes sets the default serializer for operation results. It applies
// to steps, child contexts, invokes, and condition state. Per-operation
// serdes options take precedence. The default is [JSONSerdes]. To replace
// the default from inside the handler, see [ConfigureSerdes].
func WithSerdes(s Serdes) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.serdes = s })
}

// WithCallbackDeserializer sets the default deserializer for callback
// payloads submitted by external systems. This is used when deserializing
// the result of a SUCCEEDED callback during replay. Per-operation
// [WithCallbackSerdes] takes precedence. Without it, callbacks use the
// handler-level [Serdes] set with [WithSerdes] (default: [JSONSerdes]),
// not a raw-string passthrough; see [CreateCallback]. To replace the
// default from inside the handler, see [ConfigureSerdes].
func WithCallbackDeserializer(d Deserializer) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.callbackDeserializer = d })
}

type handlerOptions struct {
	logHandler           slog.Handler
	replayLogMode        ReplayLogMode
	serdes               Serdes
	callbackDeserializer Deserializer

	// client overrides the lazily-built Lambda client. Set via
	// [WithExecutionClient].
	client ExecutionClient

	// plugins holds registered instrumentation plugins. Set via
	// [WithPlugins].
	plugins []Plugin

	// noStackTraces disables stack trace capture for failures. Set via
	// [WithStackTraces]; the zero value keeps capture enabled.
	noStackTraces bool

	// suspendSettle is how long the executing-span count must stay at
	// zero before a handler blocked on a pending operation responds
	// PENDING while other branches are still registered. The zero value
	// selects defaultSuspendSettle. Set via withSuspendSettle in tests.
	suspendSettle time.Duration
}

// defaultSuspendSettle is the default settle period for suspendSignal's
// drain. A branch that has just recorded one span may begin the next
// within a scheduler quantum, so the period is long enough to observe
// that under load, and short enough to add little to a suspension.
const defaultSuspendSettle = 20 * time.Millisecond

// withSuspendSettle overrides the settle period of the suspension drain.
// Test-only.
func withSuspendSettle(d time.Duration) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.suspendSettle = d })
}

// WithExecutionClient sets the execution client for the durable handler.
// The default client calls the AWS Lambda service and is built lazily from
// the default AWS config.
//
// Use this option to inject a custom [ExecutionClient] implementation, such
// as the in-memory client provided by the [durabletest] package for local
// testing.
func WithExecutionClient(client ExecutionClient) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.client = client })
}

// withLambdaAPI injects a Lambda client double. Test-only; retained for
// backward compatibility with existing tests that use this name.
func withLambdaAPI(client ExecutionClient) HandlerOption {
	return WithExecutionClient(client)
}

type handlerOptionFunc func(*handlerOptions)

func (f handlerOptionFunc) applyHandler(o *handlerOptions) { f(o) }

// durableHandler drives one durable invocation: parse the durable payload,
// reconstruct execution state, run the user handler under replay, and
// translate the outcome (result, failure, or suspension) into the
// invocation response.
type durableHandler[I, O any] struct {
	handler Handler[I, O]
	options handlerOptions

	clientMu sync.Mutex
	client   ExecutionClient
}

// lambdaResponseSizeLimit is the maximum serialized result size (in bytes)
// that an invocation returns inline: the 6MB Lambda response limit minus 50
// bytes for the response envelope. Results larger than this are persisted
// through a checkpoint instead, and the response carries an empty Result.
const lambdaResponseSizeLimit = 6*1024*1024 - 50

// errSuspendExecution signals that the current invocation must end with a
// PENDING response because execution is blocked on pending operations
// (waits, callbacks, in-flight invokes). It is internal control flow that
// user code never observes. Operations return it directly; wrapping is
// safe because the translation uses errors.Is, but adds no value.
var errSuspendExecution = errors.New("durable: execution suspended")

// Invoke handles one raw invocation payload: parse the durable payload,
// reconstruct execution state, run the user handler on its own goroutine
// under replay, and translate the outcome into the invocation response.
func (h *durableHandler[I, O]) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	var in wire.InvocationInput
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, fmt.Errorf("durable: parse invocation input: %w", err)
	}
	if in.DurableExecutionArn == "" || in.CheckpointToken == "" {
		return nil, errors.New("durable: invocation input missing DurableExecutionArn or CheckpointToken; is the function configured with DurableConfig?")
	}

	client, err := h.lambdaClient(ctx)
	if err != nil {
		return nil, err
	}
	cp := newCheckpointer(client, in.DurableExecutionArn, in.CheckpointToken)

	// The root branch token is released when the response is decided. The
	// handler goroutine releases it itself only when it unwinds with
	// errSuspendExecution, so that a handler blocked on a pending operation
	// counts as blocked while the outcome is decided. On every other exit,
	// including a panic, the token is held until here, so a branch that
	// commits to PENDING after the handler returned cannot change the
	// outcome. Releasing it lets the suspend signal fire once the last
	// orphaned branch deregisters, which settles any future a goroutine is
	// still blocked on. The checkpointer itself is terminated earlier, in
	// runHandler, the moment the handler's outcome is decided.
	var ec *execContext
	defer func() {
		if ec != nil {
			ec.branchTok.release()
		}
	}()

	state, err := assembleState(ctx, cp, &in.InitialExecutionState)
	if err != nil {
		// Loading state is a client call outside the handler, so a failure
		// with no stated scope is presumed transient: the invocation ends
		// with an error and the execution resumes in a later one. A client
		// that states the failure is fatal for the execution is believed.
		if failureScope(err, ErrorScopeInvocation) == ErrorScopeExecution {
			return respond(wire.InvocationResponse{
				Status: wire.StatusFailed,
				Error:  errorObjectFromError(err, nil),
			})
		}
		return nil, err
	}
	state.setUpdatedOperationIDs(in.UpdatedOperationIds)

	var event I
	if raw, ok := customerInput(&in.InitialExecutionState); ok {
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return nil, fmt.Errorf("durable: deserialize customer input: %w", err)
		}
	}

	invMeta := invocationInfoFromContext(ctx)

	// Plugin dispatcher: nil when no plugins are registered (zero overhead).
	pd := newPluginDispatcher(h.options.plugins)

	isFirstInvocation := state.numOperations() <= 1

	// Build updated operations map for both InvocationHookInfo and
	// OnOperationChange (Item 11: map[string]OperationHookInfo keyed by ID).
	var updatedOpsMap map[string]OperationHookInfo
	if pd != nil && len(in.UpdatedOperationIds) > 0 {
		updatedOpsMap = make(map[string]OperationHookInfo, len(in.UpdatedOperationIds))
		for _, uid := range in.UpdatedOperationIds {
			// UpdatedOperationIds carry wire IDs (already hashed), so look
			// up directly; state.get would hash a second time and miss.
			op := state.getByWireID(uid)
			if op == nil {
				continue
			}
			updatedOpsMap[uid] = OperationHookInfo{
				ExecutionArn:   in.DurableExecutionArn,
				ID:             uid,
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
	}

	// The root EXECUTION operation is located once, by type, and shared by
	// every path that needs it: the execution start timestamp below and
	// the oversized-result checkpoint at the end of the invocation. nil
	// when the state holds no such operation.
	execOp := state.executionOperation()

	// ExecutionStartTimestamp: sourced from the root EXECUTION operation's
	// own StartTimestamp in the wire payload.
	var execStartTimestamp time.Time
	if execOp != nil {
		execStartTimestamp = execOp.startTimestamp
	}

	// ExecutionInput: the raw unmarshaled customer event. We pass the
	// already-deserialized typed event as any.
	var execInput any = event

	invInfo := InvocationHookInfo{
		ExecutionArn:            in.DurableExecutionArn,
		IsFirstInvocation:       isFirstInvocation,
		ExecutionInput:          execInput,
		ExecutionStartTimestamp: execStartTimestamp,
		UpdatedOperations:       updatedOpsMap,
	}

	// Item 12: OnInvocationStart fires BEFORE OnOperationChange.
	dispatchNotification(pd, func(p *Plugin) {
		if p.OnInvocationStart != nil {
			p.OnInvocationStart(ctx, invInfo)
		}
	})

	// OnOperationChange: fire for operations that changed externally.
	if pd != nil && len(updatedOpsMap) > 0 {
		changeInfo := OperationChangeHookInfo{
			ExecutionArn:      in.DurableExecutionArn,
			UpdatedOperations: updatedOpsMap,
		}
		dispatchNotification(pd, func(p *Plugin) {
			if p.OnOperationChange != nil {
				p.OnOperationChange(ctx, changeInfo)
			}
		})
	}

	// The user handler runs on its own goroutine, which owns the root
	// context: durable operations are claimed there in program order.
	// Suspension is signaled out-of-band: once an operation fires the
	// suspend signal, the invocation ends with PENDING regardless of how
	// the user goroutine later unwinds, so user code cannot convert a
	// suspended execution into a completed one by intercepting the
	// suspension error. The channel is buffered so the abandoned
	// goroutine's final send never blocks.
	type outcome struct {
		result O
		err    error
		// trace is the stack trace captured when the handler failed. It
		// is recorded in the FAILED response; nil otherwise.
		trace []string
	}
	// Plugin log-context enrichment wraps the installed handler only when a
	// plugin implements the hook; see enrichLogHandler.
	ec = newExecContext(ctx, in.DurableExecutionArn, invMeta, newEnrichLogHandler(h.options.logHandler, pd), state)
	ec.checkpointer = cp
	ec.executionStartTime = execStartTimestamp
	ec.noStackTraces = h.options.noStackTraces
	cp.state = state
	if h.options.serdes != nil || h.options.callbackDeserializer != nil {
		d := ec.serdesDefaults()
		if h.options.serdes != nil {
			d.serdes = h.options.serdes
		}
		d.callbackDeserializer = h.options.callbackDeserializer
		ec.setSerdesDefaults(d)
	}
	ec.pluginDispatcher = pd
	if h.options.replayLogMode == ReplayLogModeEmit {
		l := ec.logDefaults()
		l.emitReplayed = true
		ec.setLogDefaults(l)
	}
	outcomeCh := make(chan outcome, 1)
	// handlerTrace is the stack trace of the handler's failure, copied out
	// of the outcome on this goroutine once the handler has returned.
	var handlerTrace []string

	// WrapInvocation: compose around the handler execution. The context
	// the hooks supply becomes the root context's parent; the handler
	// goroutine has not started yet, so no other goroutine reads it.
	runHandler := func(hctx context.Context) (any, error) {
		ec.Context = hctx
		// Every exit from runHandler terminates the checkpointer:
		// suspension, success, handler error, and handler panic (which
		// runUserFunc converts into an error). One defer covers them all,
		// so no exit path added later can miss it. Termination happens
		// the moment the handler's outcome is decided, before result
		// serialization, WrapInvocation post-processing, and the
		// OnInvocationEnd hooks run. A durable.Go branch still mid-flight
		// is refused at its next checkpoint attempt with
		// errCheckpointTerminated; it settles its future and releases its
		// branch token without recording further state. The invocation's
		// own record of an oversized result is written afterwards through
		// checkpointFinal, which termination does not refuse.
		defer cp.terminate()

		// Register the root handler goroutine as an active branch. The
		// goroutine deregisters itself only when the handler unwinds
		// with errSuspendExecution (blocked on a pending operation), so
		// the suspend signal can fire while the outcome is undecided. On
		// a successful, failed, or panicking return the token is held
		// until the invocation has decided its response; the deferred
		// cleanup in Invoke releases it then.
		ec.adoptBranchToken(ec.suspend.registerBranchToken())
		go func() {
			// The root context is owned by this goroutine, not the one
			// that constructed it.
			ec.owner = currentGoroutineOwner()
			result, trace, err := runUserFunc(ec, h.handler, "durable: handler panicked", func() (O, error) {
				return h.handler(ec, event)
			})
			if errors.Is(err, errSuspendExecution) {
				ec.branchTok.release()
			}
			outcomeCh <- outcome{result: result, err: err, trace: trace}
		}()

		select {
		case out := <-outcomeCh:
			handlerTrace = out.trace
			if halt := cp.haltCause(); halt != nil {
				// The service stopped accepting this invocation's
				// checkpoints before the handler returned. Whatever the
				// handler returned cannot be reported: a result or an
				// ordinary error would claim an outcome for an
				// invocation the service no longer follows. End with
				// the cause the checkpointer recorded instead:
				// errSuspendExecution (PENDING) when a response carried
				// no token, or the stale-token error (an invocation
				// failure) when a newer invocation superseded this one.
				return nil, halt
			}
			if errors.Is(out.err, errSuspendExecution) {
				// The handler goroutine is blocked on a pending
				// operation, not finished. A step or condition check
				// launched asynchronously (StepAsync, durable.Go) may
				// still be running, or a child context may be recording
				// its completion, and that outcome belongs to this
				// invocation: responding now would terminate the
				// checkpointer under it and discard the work. Wait
				// until no such span is executing and none begins for a
				// settle period, or until no branch remains that could
				// begin one. Branches that are blocked, or running code
				// between operations, are not waited for beyond that:
				// they record nothing a later invocation cannot redo.
				// The context's end bounds the wait, so a span cannot
				// hold the response past the invocation's deadline.
				settle := h.options.suspendSettle
				if settle <= 0 {
					settle = defaultSuspendSettle
				}
				ec.suspend.awaitDrain(ctx, settle)
				if halt := cp.haltCause(); halt != nil {
					// A branch's checkpoint met a stale-token
					// rejection while draining; that ends the
					// invocation with an error, as below.
					return nil, halt
				}
				return nil, errSuspendExecution
			}
			if ec.suspend.fired() || ec.suspend.committed() {
				// The handler returned while a pending commitment
				// stands: the invocation responds PENDING. Orphaned
				// branches (durable.Go children still mid-flight) are
				// not joined, so the response is immediate; the
				// checkpointer termination deferred above stops them
				// recording further state.
				return nil, errSuspendExecution
			}
			if out.err != nil {
				return nil, out.err
			}
			return out.result, nil
		case <-ec.suspend.done():
			if halt := cp.haltCause(); halt != nil {
				// Same override as above: a stale-token rejection ends
				// the invocation with an error even when every branch
				// has since blocked.
				return nil, halt
			}
			return nil, errSuspendExecution
		}
	}

	wrapResult, wrapErr := wrapChain(pd, ctx,
		func(p *Plugin) wrapHook {
			if p.WrapInvocation == nil {
				return nil
			}
			return func(hctx context.Context, fn wrapBody) (any, error) {
				return p.WrapInvocation(hctx, invInfo, fn)
			}
		},
		runHandler,
	)

	// Translate the wrapped outcome to the wire response.
	var resp []byte
	var respErr error
	switch {
	case errors.Is(wrapErr, errSuspendExecution) || ec.suspend.fired():
		// OnInvocationEnd with PENDING.
		dispatchNotification(pd, func(p *Plugin) {
			if p.OnInvocationEnd != nil {
				p.OnInvocationEnd(ctx, InvocationEndHookInfo{
					ExecutionArn: in.DurableExecutionArn,
					Status:       PluginInvocationPending,
					// ExecutionResult/ExecutionError are nil on suspension.
				})
			}
		})
		resp, respErr = respond(wire.InvocationResponse{Status: wire.StatusPending})
	case wrapErr != nil:
		dispatchNotification(pd, func(p *Plugin) {
			if p.OnInvocationEnd != nil {
				p.OnInvocationEnd(ctx, InvocationEndHookInfo{
					ExecutionArn:   in.DurableExecutionArn,
					Status:         PluginInvocationFailed,
					ExecutionError: wrapErr,
				})
			}
		})
		// A failure the handler returned is an ordinary failure of the
		// execution unless it states a scope. An invocation-scoped
		// CheckpointError or ClientError ends the invocation with an
		// error, so the execution resumes in a later invocation from its
		// last checkpoint. Everything else, including an execution-scoped
		// error, fails the execution.
		if failureScope(wrapErr, ErrorScopeExecution) == ErrorScopeInvocation {
			return nil, wrapErr
		}
		resp, respErr = respond(wire.InvocationResponse{
			Status: wire.StatusFailed,
			Error:  errorObjectFromError(wrapErr, handlerTrace),
		})
	default:
		// Serialize the result first; emit the success plugin hook only
		// after all durable work (including oversized-result checkpoint)
		// has completed. This prevents false success telemetry if
		// serialization or checkpointing fails.
		//
		// failInvocationEnd dispatches the failure-side OnInvocationEnd
		// hook for errors raised during result finalization, so lifecycle
		// telemetry observes exactly one terminal hook per invocation.
		failInvocationEnd := func(err error) {
			dispatchNotification(pd, func(p *Plugin) {
				if p.OnInvocationEnd != nil {
					p.OnInvocationEnd(ctx, InvocationEndHookInfo{
						ExecutionArn:   in.DurableExecutionArn,
						Status:         PluginInvocationFailed,
						ExecutionError: err,
					})
				}
			})
		}
		serialized, serr := json.Marshal(wrapResult)
		if serr != nil {
			err := fmt.Errorf("durable: serialize handler result: %w", serr)
			failInvocationEnd(err)
			return nil, err
		}
		if len(serialized) > lambdaResponseSizeLimit {
			// The result exceeds what the response envelope can carry
			// inline. Persist it through a checkpoint on the root
			// execution operation and return an empty Result. The
			// checkpoint must complete before responding: it is the only
			// durable copy of the result. The checkpointer was terminated
			// when the handler's outcome was decided, so this write goes
			// through checkpointFinal, the one path termination leaves
			// open to the invocation itself. The target is the EXECUTION
			// operation located by type above; its ID is already in wire
			// form, so it is passed through unhashed.
			if execOp == nil || execOp.id == "" {
				err := errors.New("durable: result exceeds response size limit and no execution operation is available to checkpoint it")
				failInvocationEnd(err)
				return nil, err
			}
			executionOpID := execOp.id
			update := OperationUpdate{
				Id:      &executionOpID,
				Type:    OperationTypeExecution,
				Action:  OperationActionSucceed,
				Payload: aws.String(string(serialized)),
			}
			if cerr := cp.checkpointFinal(ctx, []OperationUpdate{update}); cerr != nil {
				if errors.Is(cerr, errCheckpointTerminated) {
					// The response carried no token: the service will
					// accept no further checkpoints from this
					// invocation. The result may or may not have been
					// recorded, so the invocation must not claim the
					// execution finished. Respond PENDING, as for any
					// suspension; the next invocation replays and
					// reports the result.
					dispatchNotification(pd, func(p *Plugin) {
						if p.OnInvocationEnd != nil {
							p.OnInvocationEnd(ctx, InvocationEndHookInfo{
								ExecutionArn: in.DurableExecutionArn,
								Status:       PluginInvocationPending,
							})
						}
					})
					return respond(wire.InvocationResponse{Status: wire.StatusPending})
				}
				err := fmt.Errorf("durable: checkpoint oversized result: %w", cerr)
				failInvocationEnd(err)
				// The checkpoint is a client call outside the handler, so
				// a failure with no stated scope is presumed transient
				// and ends the invocation with an error. An
				// execution-scoped failure fails the execution instead.
				if failureScope(err, ErrorScopeInvocation) == ErrorScopeExecution {
					return respond(wire.InvocationResponse{
						Status: wire.StatusFailed,
						Error:  errorObjectFromError(err, nil),
					})
				}
				return nil, err
			}
			empty := ""
			resp, respErr = respond(wire.InvocationResponse{Status: wire.StatusSucceeded, Result: &empty})
		} else {
			s := string(serialized)
			resp, respErr = respond(wire.InvocationResponse{Status: wire.StatusSucceeded, Result: &s})
		}
		// OnInvocationEnd fires only after result durability is guaranteed.
		dispatchNotification(pd, func(p *Plugin) {
			if p.OnInvocationEnd != nil {
				p.OnInvocationEnd(ctx, InvocationEndHookInfo{
					ExecutionArn:    in.DurableExecutionArn,
					Status:          PluginInvocationSucceeded,
					ExecutionResult: wrapResult,
				})
			}
		})
	}
	return resp, respErr
}

// lambdaClient returns the injected client or lazily builds the default
// one. The client is memoized only on success, so a transient config
// failure on one invocation is retried on the next instead of bricking the
// warm environment.
func (h *durableHandler[I, O]) lambdaClient(ctx context.Context) (ExecutionClient, error) {
	h.clientMu.Lock()
	defer h.clientMu.Unlock()
	if h.client != nil {
		return h.client, nil
	}
	if h.options.client != nil {
		h.client = h.options.client
		return h.client, nil
	}
	client, err := defaultExecutionClient(ctx)
	if err != nil {
		return nil, err
	}
	h.client = client
	return h.client, nil
}

// assembleState combines the operation page embedded in the invocation
// payload with any remaining pages fetched from the backend.
func assembleState(ctx context.Context, cp *checkpointer, initial *wire.InitialExecutionState) (*executionState, error) {
	ops := operationsFromPayload(initial)
	if initial.NextMarker != "" {
		rest, err := cp.loadStateFrom(ctx, initial.NextMarker)
		if err != nil {
			return nil, err
		}
		ops = append(ops, rest...)
	}
	return newExecutionState(ops), nil
}

// errorObjectFromError builds the wire error object for a FAILED response.
// The ErrorType follows [wireErrorType], the same rule checkpoint updates
// use. The message is the outermost error's message, except for callback
// and child-context failures, which report their cause's message. trace
// is the stack trace captured where the handler failed; a trace already
// recorded for an operation error in the chain takes precedence, so the
// response points at the operation that failed first.
func errorObjectFromError(err error, trace []string) *wire.ErrorObject {
	rec := recordOf(err).withTrace(trace)
	we := &wire.ErrorObject{ErrorType: rec.errType, ErrorMessage: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace}
	// Callback and child-context failures report their recorded cause
	// message, so the response matches the message the operation recorded.
	switch e := outermostSDKError(err).(type) {
	case *CallbackError:
		we.ErrorMessage = causeMessage(e.operationError())
	case *CallbackExternalError:
		we.ErrorMessage = causeMessage(e.operationError())
	case *CallbackTimeoutError:
		we.ErrorMessage = causeMessage(e.operationError())
	case *CallbackSubmitterError:
		we.ErrorMessage = causeMessage(e.operationError())
	case *ChildContextError:
		we.ErrorMessage = causeMessage(e.operationError())
	}
	return we
}

// causeMessage returns the message recorded for an operation's cause: the
// Message field when set, otherwise the stand-in's message, otherwise the
// cause's own text.
func causeMessage(op *OperationError) string {
	if op.Message != "" {
		return op.Message
	}
	var re *replayedError
	if errors.As(op.Err, &re) {
		return re.message
	}
	if op.Err != nil {
		return op.Err.Error()
	}
	return ""
}

// respond serializes an invocation response.
func respond(r wire.InvocationResponse) ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("durable: marshal invocation response: %w", err)
	}
	return b, nil
}
