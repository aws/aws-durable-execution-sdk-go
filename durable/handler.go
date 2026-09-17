package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	h := &durableHandler[I, O]{handler: handler, options: options}
	return h.Invoke
}

// HandlerOption configures the durable execution handler at construction
// time.
type HandlerOption interface {
	applyHandler(*handlerOptions)
}

// WithLogger sets the logger used for SDK and context logging. The default
// logger emits structured JSON enriched with execution metadata.
func WithLogger(l Logger) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.logger = l })
}

// WithSerdes sets the default serializer for operation results. It applies
// to steps, child contexts, invokes, and condition state. Per-operation
// serdes options take precedence. The default is encoding/json.
func WithSerdes(s Serdes) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.serdes = s })
}

// WithCallbackDeserializer sets the default deserializer for callback
// payloads submitted by external systems. This is used when deserializing
// the result of a SUCCEEDED callback during replay. Per-operation
// [WithCallbackSerdes] takes precedence. Without it, callbacks use the
// handler-level serdes (default: encoding/json).
func WithCallbackDeserializer(d Deserializer) HandlerOption {
	return handlerOptionFunc(func(o *handlerOptions) { o.callbackDeserializer = d })
}

type handlerOptions struct {
	logger               Logger
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

	state, err := assembleState(ctx, cp, &in.InitialExecutionState)
	if err != nil {
		return nil, err
	}

	var event I
	if raw, ok := customerInput(&in.InitialExecutionState); ok {
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return nil, fmt.Errorf("durable: deserialize customer input: %w", err)
		}
	}

	invMeta := invocationInfoFromContext(ctx)
	logger := h.options.logger
	if logger == nil {
		logger = newDefaultLogger(in.DurableExecutionArn)
	} else {
		// Wrap user-provided loggers with replay suppression so they
		// don't emit during replay. The default logger already has this
		// built in; user loggers need the wrapper to implement
		// replayToggler.
		logger = &replayAwareLogger{inner: logger, replaying: &atomic.Bool{}}
	}

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

	// ExecutionStartTimestamp: sourced from the root EXECUTION operation's
	// own StartTimestamp in the wire payload (the first operation).
	var execStartTimestamp time.Time
	if state.numOperations() > 0 {
		state.rangeOperations(func(op *operation) bool {
			if op.opType == "EXECUTION" {
				execStartTimestamp = op.startTimestamp
				return false
			}
			return true
		})
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
	ec := newExecContext(ctx, in.DurableExecutionArn, invMeta, logger, state)
	ec.checkpointer = cp
	ec.executionStartTime = execStartTimestamp
	ec.noStackTraces = h.options.noStackTraces
	cp.state = state
	if h.options.serdes != nil {
		ec.serdes = h.options.serdes
	}
	if h.options.callbackDeserializer != nil {
		ec.callbackDeserializer = h.options.callbackDeserializer
	}
	ec.pluginDispatcher = pd
	outcomeCh := make(chan outcome, 1)
	// handlerTrace is the stack trace of the handler's failure, copied out
	// of the outcome on this goroutine once the handler has returned.
	var handlerTrace []string

	// WrapInvocation: compose around the handler execution.
	runHandler := func() (any, error) {
		// Register the root handler goroutine as an active branch.
		// Deregistration happens only when the handler unwinds with
		// errSuspendExecution (blocked on a pending operation). A
		// successful or failed handler return does not deregister:
		// the invocation completes with that outcome immediately.
		ec.branchTok = ec.suspend.registerBranchToken()
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
			if ec.suspend.fired() || ec.suspend.committed() {
				// The handler returned while a pending commitment
				// stands. Terminate the checkpointer so orphaned
				// branches (durable.Go children still mid-flight)
				// cannot record further state; they will settle their
				// futures with errSuspendExecution and release their
				// branch tokens. This does not wait for the branches
				// to finish, so the PENDING response is immediate.
				cp.terminate()
				return nil, errSuspendExecution
			}
			if out.err != nil {
				return nil, out.err
			}
			return out.result, nil
		case <-ec.suspend.done():
			cp.terminate()
			return nil, errSuspendExecution
		}
	}

	wrapResult, wrapErr := wrapChain(pd,
		func(p *Plugin) func(func() (any, error)) (any, error) {
			if p.WrapInvocation == nil {
				return nil
			}
			return func(fn func() (any, error)) (any, error) {
				return p.WrapInvocation(ctx, invInfo, fn)
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
			// durable copy of the result.
			executionOpID := ""
			if len(in.InitialExecutionState.Operations) > 0 {
				executionOpID = in.InitialExecutionState.Operations[0].Id
			}
			if executionOpID == "" {
				err := errors.New("durable: result exceeds response size limit and no execution operation is available to checkpoint it")
				failInvocationEnd(err)
				return nil, err
			}
			update := OperationUpdate{
				Id:      &executionOpID,
				Type:    OperationTypeExecution,
				Action:  OperationActionSucceed,
				Payload: aws.String(string(serialized)),
			}
			if cerr := cp.checkpoint(ctx, []OperationUpdate{update}); cerr != nil {
				err := fmt.Errorf("durable: checkpoint oversized result: %w", cerr)
				failInvocationEnd(err)
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
