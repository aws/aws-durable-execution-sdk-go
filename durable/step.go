package durable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// operationSubTypeStep is the wire subtype for step operations.
const operationSubTypeStep = "Step"

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

	options := stepOptions{retry: ExponentialBackoff(), serdes: ec.serdes}
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

	options := stepOptions{retry: ExponentialBackoff(), serdes: ec.serdes}
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

	tok := ec.suspend.registerBranchToken()
	go func() {
		defer tok.release()
		branch := ec.branch(currentGoroutineOwner())
		branch.branchTok = tok
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

	if err := validateReplayConsistency(op, string(OperationTypeStep), operationSubTypeStep, name); err != nil {
		return zero, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return zero, ec.parkUnfinishedReplay()
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
			dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
				if p.OnOperationStart != nil {
					p.OnOperationStart(ec, OperationHookInfo{
						ExecutionArn:   ec.executionArn,
						ID:             id,
						Name:           name,
						Type:           string(OperationTypeStep),
						SubType:        operationSubTypeStep,
						Status:         PluginOperationSucceeded,
						Attempt:        op.step.attempt,
						IsReplay:       true,
						ParentID:       ec.parentWireID(),
						StartTimestamp: op.startTimestamp,
						Result:         op.step.result,
					})
				}
			})
			var out O
			if err := options.serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.step.result), &out); err != nil {
				return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
			}
			dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
				if p.OnOperationEnd != nil {
					p.OnOperationEnd(ec, OperationHookInfo{
						ExecutionArn:   ec.executionArn,
						ID:             id,
						Name:           name,
						Type:           string(OperationTypeStep),
						SubType:        operationSubTypeStep,
						Status:         PluginOperationSucceeded,
						Attempt:        op.step.attempt,
						IsReplay:       true,
						ParentID:       ec.parentWireID(),
						StartTimestamp: op.startTimestamp,
						EndTimestamp:   op.endTimestamp,
						Result:         op.step.result,
					})
				}
			})
			return out, nil

		case statusFailed:
			if op.step == nil {
				return zero, fmt.Errorf("durable: step %q: checkpointed %s operation has no step details", name, op.status)
			}
			dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
				if p.OnOperationStart != nil {
					p.OnOperationStart(ec, OperationHookInfo{
						ExecutionArn:   ec.executionArn,
						ID:             id,
						Name:           name,
						Type:           string(OperationTypeStep),
						SubType:        operationSubTypeStep,
						Status:         PluginOperationFailed,
						Attempt:        op.step.attempt,
						IsReplay:       true,
						ParentID:       ec.parentWireID(),
						StartTimestamp: op.startTimestamp,
						Error:          &replayedError{errType: op.step.errType, message: op.step.errMessage},
					})
				}
			})
			stepErr := &StepError{
				Name:     name,
				Attempts: op.step.attempt,
				Err:      &replayedError{errType: op.step.errType, message: op.step.errMessage},
			}
			dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
				if p.OnOperationEnd != nil {
					p.OnOperationEnd(ec, OperationHookInfo{
						ExecutionArn:   ec.executionArn,
						ID:             id,
						Name:           name,
						Type:           string(OperationTypeStep),
						SubType:        operationSubTypeStep,
						Status:         PluginOperationFailed,
						Attempt:        op.step.attempt,
						IsReplay:       true,
						ParentID:       ec.parentWireID(),
						StartTimestamp: op.startTimestamp,
						EndTimestamp:   op.endTimestamp,
						Error:          stepErr.Err,
					})
				}
			})
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
				return settleStepFailure[O](ec, id, name, options, &StepInterruptedError{Name: name}, attempt)
			}

		case statusReady, statusCancelled, statusTimedOut, statusStopped:
			// READY executes below. The remaining statuses are not
			// produced for step operations; execute and let the
			// backend reject the update if the state is invalid.
		}
	}

	// OnOperationStart for live execution.
	startTime := time.Now()
	dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
		if p.OnOperationStart != nil {
			p.OnOperationStart(ec, OperationHookInfo{
				ExecutionArn:   ec.executionArn,
				ID:             id,
				Name:           name,
				Type:           string(OperationTypeStep),
				SubType:        operationSubTypeStep,
				Status:         PluginOperationStarted,
				Attempt:        attempt,
				IsReplay:       isReplay,
				ParentID:       ec.parentWireID(),
				StartTimestamp: startTime,
			})
		}
	})

	result, err := executeStepAttempt(ec, id, name, fn, options, op, attempt)

	// OnOperationEnd for live execution.
	if err == nil {
		dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
			if p.OnOperationEnd != nil {
				p.OnOperationEnd(ec, OperationHookInfo{
					ExecutionArn:   ec.executionArn,
					ID:             id,
					Name:           name,
					Type:           string(OperationTypeStep),
					SubType:        operationSubTypeStep,
					Status:         PluginOperationSucceeded,
					Attempt:        attempt,
					IsReplay:       isReplay,
					ParentID:       ec.parentWireID(),
					StartTimestamp: startTime,
					EndTimestamp:   time.Now(),
				})
			}
		})
	} else if !errors.Is(err, errSuspendExecution) {
		dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
			if p.OnOperationEnd != nil {
				p.OnOperationEnd(ec, OperationHookInfo{
					ExecutionArn:   ec.executionArn,
					ID:             id,
					Name:           name,
					Type:           string(OperationTypeStep),
					SubType:        operationSubTypeStep,
					Status:         PluginOperationFailed,
					Attempt:        attempt,
					IsReplay:       isReplay,
					ParentID:       ec.parentWireID(),
					StartTimestamp: startTime,
					EndTimestamp:   time.Now(),
					Error:          err,
				})
			}
		})
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
			ExecutionArn:   ec.executionArn,
			ID:             id,
			Name:           name,
			Type:           string(OperationTypeStep),
			SubType:        operationSubTypeStep,
			Status:         PluginOperationStarted,
			Attempt:        attempt,
			IsReplay:       ec.IsReplaying(),
			ParentID:       ec.parentWireID(),
			StartTimestamp: time.Now(),
		},
		Attempt: attempt,
	}

	// OnOperationAttemptStart
	dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
		if p.OnOperationAttemptStart != nil {
			p.OnOperationAttemptStart(ec, attemptInfo)
		}
	})

	// WrapOperationAttemptFn wraps the step body execution.
	var result O
	var stepErr error

	wrappedResult, wrappedErr := wrapChain(ec.pluginDispatcher,
		func(p *Plugin) func(func() (any, error)) (any, error) {
			if p.WrapOperationAttemptFn == nil {
				return nil
			}
			return func(innerFn func() (any, error)) (any, error) {
				return p.WrapOperationAttemptFn(ec, attemptInfo, innerFn)
			}
		},
		func() (any, error) {
			r, e := runStepFunc(ec, fn, attempt)
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
		dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
			if p.OnOperationAttemptEnd != nil {
				p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
					OperationHookInfo: attemptInfo.OperationHookInfo,
					Attempt:           attempt,
					Outcome:           PluginAttemptFailed,
					Error:             stepErr,
				})
			}
		})
		return settleStepFailure[O](ec, id, name, options, stepErr, attempt)
	}

	serialized, err := options.serdes.Marshal(ec.Context, ec.serdesCtx(id), result)
	if err != nil {
		wrapped := newSerdesError(name, serdesDirectionMarshal, err)
		dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
			if p.OnOperationAttemptEnd != nil {
				p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
					OperationHookInfo: attemptInfo.OperationHookInfo,
					Attempt:           attempt,
					Outcome:           PluginAttemptFailed,
					Error:             wrapped,
				})
			}
		})
		return settleStepFailure[O](ec, id, name, options, wrapped, attempt)
	}

	if sizeErr := checkResultSize(serialized, name); sizeErr != nil {
		dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
			if p.OnOperationAttemptEnd != nil {
				p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
					OperationHookInfo: attemptInfo.OperationHookInfo,
					Attempt:           attempt,
					Outcome:           PluginAttemptFailed,
					Error:             sizeErr,
				})
			}
		})
		return settleStepFailure[O](ec, id, name, options, sizeErr, attempt)
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
	dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
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
// invocation) or FAIL when retries are exhausted.
func settleStepFailure[O any](ec *execContext, id, name string, options stepOptions, cause error, attempt int) (O, error) {
	var zero O

	decision := options.retry(cause, attempt)
	if !decision.Retry {
		update := stepUpdate(ec, id, name, OperationActionFail)
		update.Error = errorObject(cause)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			if errors.Is(err, errCheckpointTerminated) {
				return zero, errSuspendExecution
			}
			return zero, err
		}
		return zero, &StepError{Name: name, Attempts: attempt, Err: cause}
	}

	update := stepUpdate(ec, id, name, OperationActionRetry)
	update.Error = errorObject(cause)
	delaySec, delayErr := durationToSeconds(decision.Delay)
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
func runStepFunc[O any](ec *execContext, fn func(StepContext) (O, error), attempt int) (result O, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("durable: step panicked: %v", r)
		}
	}()
	return fn(&stepContext{Context: ec.Context, logger: ec.logger, attempt: attempt})
}

// stepUpdate assembles the shared fields of a step operation update. IDs
// are hashed to their wire form.
func stepUpdate(ec *execContext, id, name string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeStep,
		SubType: aws.String(operationSubTypeStep),
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

// stepContext is the concrete [StepContext] passed to step bodies.
type stepContext struct {
	context.Context
	logger  Logger
	attempt int
}

var _ StepContext = (*stepContext)(nil)

func (c *stepContext) Logger() Logger { return c.logger }

// Attempt returns the 1-based attempt number for this step execution.
func (c *stepContext) Attempt() int { return c.attempt }

// errorObject converts a Go error into the wire error shape recorded with
// FAIL and RETRY updates.
func errorObject(err error) *ErrorObject {
	return &ErrorObject{
		ErrorType:    aws.String(errorTypeName(err)),
		ErrorMessage: aws.String(err.Error()),
	}
}

// errorTypeName derives the wire ErrorType from an error's concrete type
// name, so retry strategies keyed on error identity see a stable name.
// Unnamed error types (such as those from [errors.New] and [fmt.Errorf])
// map to "Error".
func errorTypeName(err error) string {
	t := reflect.TypeOf(err)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return "Error"
	}
	name := t.Name()
	if name == "" || name == "errorString" || name == "wrapError" || name == "joinError" {
		return "Error"
	}
	return name
}

// replayedError reconstructs a failure recorded in a checkpoint, for
// replaying a FAILED step without re-executing it.
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
