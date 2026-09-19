package durable

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// StepSemantics selects a step's execution guarantee across retries.
type StepSemantics int

// Step execution guarantees.
const (
	// AtLeastOncePerRetry re-executes a step whose previous invocation
	// was interrupted before recording an outcome. The step body may run
	// more than once per retry attempt. This is the default.
	AtLeastOncePerRetry StepSemantics = iota

	// AtMostOncePerRetry never re-executes an interrupted attempt.
	// Interruption is treated as a failed attempt and consumes one retry
	// from the step's retry strategy.
	AtMostOncePerRetry
)

// StepOption configures a single step operation.
type StepOption interface {
	applyStep(*stepOptions)
}

// WithRetry sets the step's retry strategy. The default is
// [ExponentialBackoff].
func WithRetry(s RetryStrategy) StepOption {
	return stepOptionFunc(func(o *stepOptions) { o.retry = s })
}

// WithStepSerdes overrides the serializer for this step's result.
func WithStepSerdes(s Serdes) StepOption {
	return stepOptionFunc(func(o *stepOptions) { o.serdes = s })
}

// WithSemantics sets the step's execution guarantee. The default is
// [AtLeastOncePerRetry].
func WithSemantics(s StepSemantics) StepOption {
	return stepOptionFunc(func(o *stepOptions) { o.semantics = s })
}

type stepOptions struct {
	retry     RetryStrategy
	serdes    Serdes
	semantics StepSemantics
}

type stepOptionFunc func(*stepOptions)

func (f stepOptionFunc) applyStep(o *stepOptions) { f(o) }

// Step executes fn as a durable step: its result is checkpointed, and on
// replay the stored result is returned without re-executing fn. name
// identifies the step for tracking and debugging; pass "" for an unnamed
// step.
//
// A step is a single atomic unit of work and must not create durable
// operations. To group durable operations, use [RunInChildContext] or [Go].
//
// If fn fails and its retry strategy schedules another attempt, the
// execution suspends and resumes in a new invocation when the retry delay
// elapses. If fn fails after exhausting its retry strategy, Step returns a
// [*StepError].
func Step[O any](ctx Context, name string, fn func(StepContext) (O, error), opts ...StepOption) (O, error) {
	var zero O
	ec, ok := ctx.(*execContext)
	if !ok {
		return zero, fmt.Errorf("durable: Step %q: Context was not created by the SDK", name)
	}

	options := stepOptions{retry: ExponentialBackoff(), serdes: ec.serdesDefaults().serdes}
	for _, o := range opts {
		o.applyStep(&options)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}
	return runStep(ec, id, name, fn, options)
}

// StepAsync is [Step], except that fn runs concurrently and the result is
// delivered through the returned future. The operation's identity is
// claimed before StepAsync returns, so consecutive StepAsync calls from one
// goroutine are replay-deterministic.
//
// On invocation suspension, the returned future is settled with
// errSuspendExecution so goroutines blocked on [Future.Result] unwind.
func StepAsync[O any](ctx Context, name string, fn func(StepContext) (O, error), opts ...StepOption) *Future[O] {
	ec, ok := ctx.(*execContext)
	if !ok {
		return newFailedFuture[O](fmt.Errorf("durable: StepAsync %q: Context was not created by the SDK", name))
	}

	options := stepOptions{retry: ExponentialBackoff(), serdes: ec.serdesDefaults().serdes}
	for _, o := range opts {
		o.applyStep(&options)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return newFailedFuture[O](err)
	}
	if ec.unfinishedInSucceededContext(ec.state.get(id)) {
		return newUnfinishedReplayFuture[O](ec.suspend)
	}

	fut := newFuture[O]()
	registerFuture(ec.suspend, fut)

	// Snapshot the serializer and logging defaults on the owning goroutine:
	// the owner may call ConfigureSerdes or ConfigureLogging before the
	// goroutine below runs.
	defaults := ec.inheritedDefaults()
	tok := ec.suspend.registerBranchToken()
	go func() {
		defer tok.release()
		branch := ec.branchWith(currentGoroutineOwner(), defaults)
		branch.adoptBranchToken(tok)
		result, runErr := runStep(branch, id, name, fn, options)
		fut.settle(result, runErr)
	}()

	return fut
}

// runStep drives one step operation from its checkpointed status: return a
// terminal outcome from replay, suspend on a scheduled retry, or execute an
// attempt.
func runStep[O any](ec *execContext, id, name string, fn func(StepContext) (O, error), options stepOptions) (O, error) {
	var zero O
	op := ec.state.get(id)

	if err := validateReplayConsistency(op, string(OperationTypeStep), OperationSubTypeStep, name); err != nil {
		return zero, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return zero, ec.parkUnfinishedReplay(op, id, string(OperationTypeStep), OperationSubTypeStep, name)
	}

	attempt := 1
	if op != nil && op.step != nil {
		attempt = op.step.attempt + 1
	}

	currentMode := executionMode(ec.mode.Load())
	isReplay := currentMode == modeReplay || currentMode == modeReplaySucceededContext

	if op != nil {
		switch op.status {
		case statusSucceeded:
			if op.step == nil {
				return zero, fmt.Errorf("durable: step %q: checkpointed %s operation has no step details", name, op.status)
			}
			// Fire operation hooks for replayed terminal operations.
			info := ec.operationHookInfo(id, name, string(OperationTypeStep), OperationSubTypeStep, true)
			info.Attempt = op.step.attempt
			info.StartTimestamp = op.startTimestamp
			info.Result = op.step.result
			dispatchOperationStart(ec, info, PluginOperationSucceeded)
			var out O
			if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.step.result), &out); err != nil {
				return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
			}
			info.EndTimestamp = op.endTimestamp
			dispatchOperationEnd(ec, info, PluginOperationSucceeded)
			return out, nil

		case statusFailed:
			if op.step == nil {
				return zero, fmt.Errorf("durable: step %q: checkpointed %s operation has no step details", name, op.status)
			}
			info := ec.operationHookInfo(id, name, string(OperationTypeStep), OperationSubTypeStep, true)
			info.Attempt = op.step.attempt
			info.StartTimestamp = op.startTimestamp
			info.Error = op.step.record().standIn(nil)
			dispatchOperationStart(ec, info, PluginOperationFailed)
			stepErr := newStepError(name, op.step.attempt, op.step.record())
			info.EndTimestamp = op.endTimestamp
			info.Error = stepErr.Err
			dispatchOperationEnd(ec, info, PluginOperationFailed)
			return zero, stepErr

		case statusPending:
			// A retry is scheduled and its timer has not fired.
			// Suspend; the backend re-invokes when the attempt is due.
			// Operation hooks NOT fired for still-pending operations.
			ec.blocked.Store(true)
			ec.suspend.commitPending(ec.abandon)
			return zero, errSuspendExecution

		case statusStarted:
			if options.semantics == AtMostOncePerRetry {
				// The previous attempt was interrupted before
				// recording an outcome and must not re-execute.
				return settleStepFailure[O](ec, id, name, options, &StepInterruptedError{Name: name}, nil, attempt)
			}

		case statusReady, statusCancelled, statusTimedOut, statusStopped:
			// READY executes below. The remaining statuses are not
			// produced for step operations; execute and let the
			// backend reject the update if the state is invalid.
		}
	}

	// OnOperationStart for live execution.
	startTime := time.Now()
	liveInfo := ec.operationHookInfo(id, name, string(OperationTypeStep), OperationSubTypeStep, isReplay)
	liveInfo.Attempt = attempt
	liveInfo.StartTimestamp = startTime
	dispatchOperationStart(ec, liveInfo, PluginOperationStarted)

	// The attempt counts as executing from its START checkpoint through
	// the checkpoint of its outcome, so an invocation whose handler blocks
	// meanwhile waits for the outcome to be recorded before it suspends.
	result, err := func() (O, error) {
		ec.suspend.enterExecuting()
		defer ec.suspend.exitExecuting()
		return executeStepAttempt(ec, id, name, fn, options, op, attempt)
	}()

	// OnOperationEnd for live execution.
	if err == nil {
		liveInfo.EndTimestamp = time.Now()
		dispatchOperationEnd(ec, liveInfo, PluginOperationSucceeded)
	} else if !errors.Is(err, errSuspendExecution) {
		liveInfo.EndTimestamp = time.Now()
		liveInfo.Error = err
		dispatchOperationEnd(ec, liveInfo, PluginOperationFailed)
	}
	// If errSuspendExecution: operation is still pending, no OnOperationEnd.

	return result, err
}

// executeStepAttempt runs one attempt of the step body and checkpoints its
// outcome.
func executeStepAttempt[O any](ec *execContext, id, name string, fn func(StepContext) (O, error), options stepOptions, op *operation, attempt int) (O, error) {
	var zero O

	if op == nil || op.status != statusStarted {
		update := stepUpdate(ec, id, name, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			if errors.Is(err, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
			return zero, err
		}
	}

	attemptInfo := AttemptHookInfo{
		OperationHookInfo: OperationHookInfo{
			ExecutionArn:    ec.executionArn,
			ID:              id,
			Name:            name,
			Type:            string(OperationTypeStep),
			SubType:         OperationSubTypeStep,
			Status:          PluginOperationStarted,
			Attempt:         attempt,
			IsReplay:        ec.IsReplaying(),
			ParentID:        ec.parentWireID(),
			StartTimestamp:  time.Now(),
			ChildrenOmitted: ec.childrenOmittedAt(ec.hookDepth),
		},
		Attempt: attempt,
	}

	// OnOperationAttemptStart
	dispatchNotification(ec.operationHooks(), func(p *Plugin) {
		if p.OnOperationAttemptStart != nil {
			p.OnOperationAttemptStart(ec, attemptInfo)
		}
	})

	// WrapOperationAttemptFn wraps the step body execution.
	var result O
	var stepErr error
	// stepTrace is the stack trace captured when the step body failed. It
	// is recorded with the failure; nil when the body succeeded or capture
	// is disabled.
	var stepTrace []string

	wrappedResult, wrappedErr := wrapChain(ec.operationHooks(), ec,
		func(p *Plugin) wrapHook {
			if p.WrapOperationAttemptFn == nil {
				return nil
			}
			return func(ctx context.Context, innerFn wrapBody) (any, error) {
				return p.WrapOperationAttemptFn(ctx, attemptInfo, innerFn)
			}
		},
		func(ctx context.Context) (any, error) {
			r, trace, e := runStepFunc(ctx, ec, id, name, fn, attempt)
			stepTrace = trace
			return r, e
		},
	)
	if wrappedErr != nil {
		stepErr = wrappedErr
	} else if wrappedResult != nil {
		result, _ = wrappedResult.(O)
	}

	if stepErr != nil {
		// OnOperationAttemptEnd with FAILED outcome.
		dispatchNotification(ec.operationHooks(), func(p *Plugin) {
			if p.OnOperationAttemptEnd != nil {
				p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
					OperationHookInfo: attemptInfo.OperationHookInfo,
					Attempt:           attempt,
					Outcome:           PluginAttemptFailed,
					Error:             stepErr,
				})
			}
		})
		return settleStepFailure[O](ec, id, name, options, stepErr, stepTrace, attempt)
	}

	serialized, err := options.serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
	if err != nil {
		wrapped := newSerdesError(name, serdesDirectionMarshal, err)
		dispatchNotification(ec.operationHooks(), func(p *Plugin) {
			if p.OnOperationAttemptEnd != nil {
				p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
					OperationHookInfo: attemptInfo.OperationHookInfo,
					Attempt:           attempt,
					Outcome:           PluginAttemptFailed,
					Error:             wrapped,
				})
			}
		})
		return settleStepFailure[O](ec, id, name, options, wrapped, nil, attempt)
	}

	if sizeErr := checkResultSize(serialized, name); sizeErr != nil {
		dispatchNotification(ec.operationHooks(), func(p *Plugin) {
			if p.OnOperationAttemptEnd != nil {
				p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
					OperationHookInfo: attemptInfo.OperationHookInfo,
					Attempt:           attempt,
					Outcome:           PluginAttemptFailed,
					Error:             sizeErr,
				})
			}
		})
		return settleStepFailure[O](ec, id, name, options, sizeErr, nil, attempt)
	}

	update := stepUpdate(ec, id, name, OperationActionSucceed)
	update.Payload = aws.String(string(serialized))
	if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
		if errors.Is(cerr, errCheckpointTerminated) {
			return zero, errSuspendExecution
		}
		return zero, cerr
	}

	// OnOperationAttemptEnd with SUCCEEDED outcome.
	dispatchNotification(ec.operationHooks(), func(p *Plugin) {
		if p.OnOperationAttemptEnd != nil {
			p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
				OperationHookInfo: attemptInfo.OperationHookInfo,
				Attempt:           attempt,
				Outcome:           PluginAttemptSucceeded,
			})
		}
	})

	// Return the value as it will be seen on replay: deserialized from
	// the checkpointed payload, so first execution and replay observe an
	// identical result.
	var out O
	if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), serialized, &out); err != nil {
		return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
	}
	return out, nil
}

// settleStepFailure consults the retry strategy for a failed attempt and
// checkpoints the outcome: RETRY with a delay (then suspends the
// invocation) or FAIL when retries are exhausted. trace is the stack trace
// captured where the step body failed; it is recorded with the failure.
func settleStepFailure[O any](ec *execContext, id, name string, options stepOptions, cause error, trace []string, attempt int) (O, error) {
	var zero O

	rec := recordOf(cause).withTrace(trace)
	decision := options.retry(RetryAttempt{Err: cause, Attempt: attempt})
	if !decision.Retry {
		update := stepUpdate(ec, id, name, OperationActionFail)
		update.Error = errorObjectFromRecord(rec)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			if errors.Is(err, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
			return zero, err
		}
		return zero, newStepError(name, attempt, rec)
	}

	update := stepUpdate(ec, id, name, OperationActionRetry)
	update.Error = errorObjectFromRecord(rec)
	// A strategy that retries without choosing a delay gets the documented
	// default rather than a zero delay, whose scheduling is unspecified.
	delay := decision.Delay
	if delay == 0 {
		delay = DefaultRetryDelay
	}
	delaySec, delayErr := durationToSeconds(delay)
	if delayErr != nil {
		return zero, fmt.Errorf("durable: step %q: retry delay: %w", name, delayErr)
	}
	update.StepOptions = &StepOptions{
		NextAttemptDelaySeconds: aws.Int32(delaySec),
	}
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
		if errors.Is(err, errCheckpointTerminated) {
			return zero, errSuspendExecution
		}
		return zero, err
	}

	// The backend owns the retry timer: suspend and resume in a new
	// invocation once the delay elapses.
	ec.blocked.Store(true)
	ec.suspend.commitPending(ec.abandon)
	return zero, errSuspendExecution
}

// runStepFunc executes the step body with panic recovery: a panicking step
// is a failed attempt, subject to the retry strategy like any other error.
// ctx is the parent of the body's [StepContext]: ec's context, or the one
// the wrap hooks supplied. trace is the stack trace of the failure as
// [runUserFunc] captures it, nil when the body succeeds or when capture is
// disabled.
func runStepFunc[O any](ctx context.Context, ec *execContext, id, name string, fn func(StepContext) (O, error), attempt int) (O, []string, error) {
	return runUserFunc(ec, fn, "durable: step panicked", func() (O, error) {
		return fn(&stepContext{Context: ctx, logger: ec.operationLogger(id, name, attempt), attempt: attempt})
	})
}

// stepUpdate assembles the shared fields of a step operation update. IDs
// are hashed to their wire form.
func stepUpdate(ec *execContext, id, name string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeStep,
		SubType: aws.String(OperationSubTypeStep),
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

// stepContext is the concrete [StepContext] passed to step bodies.
type stepContext struct {
	context.Context
	logger  *slog.Logger
	attempt int
}

// Compile-time assertion that stepContext implements StepContext. It fails
// to build if stepContext loses a method the interface requires. It does not
// detect removal of sealed from the interface alone, because a concrete type
// may carry methods its interface omits. TestStepContextSealedVarSatisfied
// checks the interface side by reflection.
var _ StepContext = (*stepContext)(nil)

func (c *stepContext) Logger() *slog.Logger { return c.logger }

// Attempt returns the 1-based attempt number of the current execution of
// the user function: a step body, a condition check, or a callback
// submitter.
func (c *stepContext) Attempt() int { return c.attempt }

// sealed marks stepContext as the SDK's StepContext implementation.
func (c *stepContext) sealed() {}

// errorObject converts a Go error into the wire error shape recorded with
// FAIL and RETRY updates. The ErrorType follows [wireErrorType], the same
// rule the FAILED invocation response uses. ErrorData carries the payload
// attached with [WithErrorData], when the chain holds one.
func errorObject(err error) *ErrorObject {
	return errorObjectFromRecord(recordOf(err))
}

// errorObjectFromRecord converts a failure record into the wire error
// shape. Empty fields are omitted.
func errorObjectFromRecord(rec errorRecord) *ErrorObject {
	obj := &ErrorObject{
		ErrorType:    aws.String(rec.errType),
		ErrorMessage: aws.String(rec.message),
	}
	if rec.data != "" {
		obj.ErrorData = aws.String(rec.data)
	}
	if len(rec.stackTrace) > 0 {
		obj.StackTrace = rec.stackTrace
	}
	return obj
}

// newStepError builds the [StepError] for a failed step from the failure
// record, on both the first invocation and on replay.
func newStepError(name string, attempts int, rec errorRecord) *StepError {
	return &StepError{
		Name: name, Attempts: attempts,
		ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace,
		Err: rec.cause("", nil),
	}
}

// replayedError is the stand-in for a recorded failure. It carries the
// wire ErrorType and message of the error that escaped an operation, and
// nothing else, so a typed operation error wrapping it looks the same on
// the first invocation as on replay. Its Error() is "<ErrorType>: <message>".
type replayedError struct {
	errType  string
	message  string
	sentinel error // optional sentinel for errors.Is matching
}

var _ error = (*replayedError)(nil)

func (e *replayedError) Error() string {
	if e.errType == "" {
		return e.message
	}
	return e.errType + ": " + e.message
}

func (e *replayedError) Unwrap() error { return e.sentinel }
