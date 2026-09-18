package durable

import (
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// operationSubTypeWaitForCondition is the wire subtype for wait-for-condition
// operations.
const operationSubTypeWaitForCondition = "WaitForCondition"

// Default wait strategy parameters substituted when
// [ConditionConfig].WaitStrategy is nil.
const (
	defaultConditionMaxAttempts     = 60
	defaultConditionInitialDelaySec = 5.0
	defaultConditionMaxDelaySec     = 300.0
	defaultConditionBackoffRate     = 1.5
	defaultConditionJitterStrategy  = JitterFull
)

// defaultConditionWaitStrategy is the wait strategy used when
// ConditionConfig.WaitStrategy is nil: keep polling with exponential
// backoff (5 second initial delay, backoff rate 1.5, capped at 300
// seconds, full jitter) and fail once 60 attempts have been made.
func defaultConditionWaitStrategy[S any](_ S, attempt int) WaitDecision {
	if attempt >= defaultConditionMaxAttempts {
		return WaitDecision{
			Err: fmt.Errorf("durable: WaitForCondition exceeded maximum attempts (%d)", defaultConditionMaxAttempts),
		}
	}
	base := math.Min(
		defaultConditionInitialDelaySec*math.Pow(defaultConditionBackoffRate, float64(attempt-1)),
		defaultConditionMaxDelaySec,
	)
	return WaitDecision{Continue: true, Delay: finalizeDelay(base, defaultConditionJitterStrategy)}
}

// WaitForCondition polls check until the configured wait strategy stops,
// checkpointing the state between attempts and suspending for the strategy's
// delay. It returns the final state.
//
// On each invocation cycle the SDK re-executes check with the previously
// checkpointed state (or InitialState for the first attempt). The wait
// strategy observes the round-tripped state and the 1-based attempt number
// and decides whether to continue waiting or stop.
//
// If check returns an error, the operation fails immediately: there is no
// internal retry. The error is checkpointed and returned as a
// [*WaitForConditionError].
//
// If the wait strategy's Continue field is true, its Delay determines how
// long the execution suspends before re-invoking. When Continue is false, the
// final state is checkpointed and returned.
//
// Every state that check returns is serialized and checkpointed, whether the
// strategy continues or stops. So the result size limit applies to each
// intermediate state as well as to the final result. When a serialized state
// exceeds the limit, WaitForCondition returns a [*ResultTooLargeError]
// without checkpointing that state.
func WaitForCondition[S any](ctx Context, name string, check func(StepContext, S) (S, error), cfg ConditionConfig[S]) (S, error) {
	var zero S
	ec, ok := ctx.(*execContext)
	if !ok {
		return zero, fmt.Errorf("durable: WaitForCondition %q: Context was not created by the SDK", name)
	}

	serdes := cfg.Serdes
	if serdes == nil {
		serdes = ec.serdes
	}

	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}

	return runWaitForCondition(ec, id, name, check, cfg, serdes)
}

// runWaitForCondition drives one wait-for-condition operation from its
// checkpointed status.
func runWaitForCondition[S any](ec *execContext, id, name string, check func(StepContext, S) (S, error), cfg ConditionConfig[S], serdes Serdes) (S, error) {
	var zero S
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeStep), operationSubTypeWaitForCondition, name); err != nil {
		return zero, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return zero, ec.parkUnfinishedReplay(op, id, string(OperationTypeStep), operationSubTypeWaitForCondition, name)
	}

	// Determine the current attempt number. The checkpointed Attempt
	// field counts completed attempts; the next execution is attempt+1.
	attempt := 1
	if op != nil && op.step != nil {
		attempt = op.step.attempt + 1
	}

	if op != nil {
		switch op.status {
		case statusSucceeded:
			// Terminal success: deserialize the final state without
			// re-executing the check function.
			if op.step == nil {
				return zero, fmt.Errorf("durable: WaitForCondition %q: checkpointed %s operation has no step details", name, op.status)
			}
			var out S
			if err := serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.step.result), &out); err != nil {
				return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
			}
			return out, nil

		case statusFailed:
			// Terminal failure: reconstruct the error from the
			// checkpoint without re-executing.
			if op.step == nil {
				return zero, fmt.Errorf("durable: WaitForCondition %q: checkpointed %s operation has no step details", name, op.status)
			}
			return zero, newWaitForConditionError(name, op.step.attempt, op.step.record())

		case statusPending:
			// A retry (continue) is scheduled and its timer has not
			// fired. Suspend; the backend re-invokes when the delay
			// elapses.
			ec.blocked.Store(true)
			ec.suspend.commitPending(ec.abandon)
			return zero, errSuspendExecution

		case statusStarted, statusReady:
			// STARTED or READY: the backend has re-invoked after the
			// previous cycle's timer. Re-execute the check below.

		case statusCancelled, statusTimedOut, statusStopped:
			// These are not produced for step operations; execute
			// and let the backend reject if invalid.
		}
	}

	// The cycle counts as executing from its START checkpoint through the
	// checkpoint of its outcome; see runStep for the reason.
	ec.suspend.enterExecuting()
	defer ec.suspend.exitExecuting()
	return executeWaitForConditionAttempt(ec, id, name, check, cfg, serdes, op, attempt)
}

// executeWaitForConditionAttempt runs one cycle: checkpoint START (if not
// already started), deserialize current state, call check, consult the
// wait strategy, and checkpoint the outcome.
func executeWaitForConditionAttempt[S any](ec *execContext, id, name string, check func(StepContext, S) (S, error), cfg ConditionConfig[S], serdes Serdes, op *operation, attempt int) (S, error) {
	var zero S

	// Checkpoint START if this is a new attempt. If status is already
	// STARTED the previous invocation already checkpointed it.
	if op == nil || op.status != statusStarted {
		update := waitForConditionUpdate(ec, id, name, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			return zero, err
		}
	}

	// Determine the current state: deserialize from the checkpoint
	// (resuming after a continue) or use the initial state.
	var currentState S
	if op != nil && op.step != nil && op.step.result != "" {
		if err := serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.step.result), &currentState); err != nil {
			// The checkpointed state cannot be reconstructed; the
			// operation cannot continue with a consistent view of it.
			return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
		}
	} else {
		currentState = cfg.InitialState
	}

	attemptInfo := AttemptHookInfo{
		OperationHookInfo: OperationHookInfo{
			ExecutionArn:   ec.executionArn,
			ID:             id,
			Name:           name,
			Type:           string(OperationTypeStep),
			SubType:        operationSubTypeWaitForCondition,
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

	// WrapOperationAttemptFn wraps the check function execution.
	var newState S
	var checkErr error
	// checkTrace is the stack trace captured when the check failed; nil
	// when it succeeded or capture is disabled.
	var checkTrace []string

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
			s, trace, e := runCheckFunc(ec, check, currentState)
			checkTrace = trace
			return s, e
		},
	)
	if wrappedErr != nil {
		checkErr = wrappedErr
	} else if wrappedResult != nil {
		newState, _ = wrappedResult.(S)
	}

	// Execute the check function.
	if checkErr != nil {
		// OnOperationAttemptEnd with FAILED outcome.
		dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
			if p.OnOperationAttemptEnd != nil {
				p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
					OperationHookInfo: attemptInfo.OperationHookInfo,
					Attempt:           attempt,
					Outcome:           PluginAttemptFailed,
					Error:             checkErr,
				})
			}
		})
		// Check function failure: checkpoint FAIL and return error. The
		// checkpointed ErrorType stays the cause's concrete type name;
		// the wrapping applies only to the returned Go error.
		rec := recordOf(checkErr).withTrace(checkTrace)
		update := waitForConditionUpdate(ec, id, name, OperationActionFail)
		update.Error = errorObjectFromRecord(rec)
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			return zero, cerr
		}
		return zero, newWaitForConditionError(name, attempt, rec)
	}

	// Serialize the new state for checkpointing.
	serialized, err := serdes.Marshal(ec.Context, ec.serdesCtx(id), newState)
	if err != nil {
		return zero, newSerdesError(name, serdesDirectionMarshal, err)
	}

	// OnOperationAttemptEnd with SUCCEEDED outcome (check ran without error).
	dispatchNotification(ec.pluginDispatcher, func(p *Plugin) {
		if p.OnOperationAttemptEnd != nil {
			p.OnOperationAttemptEnd(ec, AttemptEndHookInfo{
				OperationHookInfo: attemptInfo.OperationHookInfo,
				Attempt:           attempt,
				Outcome:           PluginAttemptSucceeded,
			})
		}
	})

	// Round-trip through serdes so the wait strategy and the returned
	// value see the same representation that replay will produce.
	var deserialized S
	if err := serdes.Unmarshal(ec.Context, ec.serdesCtx(id), serialized, &deserialized); err != nil {
		return zero, newSerdesError(name, serdesDirectionUnmarshal, err)
	}

	// Consult the wait strategy with the deserialized state, substituting
	// the documented default strategy when none is configured.
	strategy := cfg.WaitStrategy
	if strategy == nil {
		strategy = defaultConditionWaitStrategy[S]
	}
	decision := strategy(deserialized, attempt)

	if !decision.Continue {
		// Strategy signaled failure (e.g., max attempts exceeded):
		// checkpoint FAIL and return error.
		if decision.Err != nil {
			// The strategy returned the error to this frame, so the
			// trace names the strategy and runs outward from here.
			rec := recordOf(decision.Err).withTrace(ec.returnedErrorTrace(strategy, decision.Err, 0))
			update := waitForConditionUpdate(ec, id, name, OperationActionFail)
			update.Error = errorObjectFromRecord(rec)
			if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
				return zero, cerr
			}
			return zero, newWaitForConditionError(name, attempt, rec)
		}

		// Condition met: checkpoint terminal SUCCEED with the final state.
		if err := checkResultSize(serialized, name); err != nil {
			return zero, err
		}
		update := waitForConditionUpdate(ec, id, name, OperationActionSucceed)
		update.Payload = aws.String(string(serialized))
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			return zero, cerr
		}
		return deserialized, nil
	}

	// Condition not met: checkpoint RETRY with the intermediate state and
	// the strategy's delay, then suspend. The intermediate state is a
	// checkpoint payload like the final result, so the same size limit
	// applies to it.
	if err := checkResultSize(serialized, name); err != nil {
		return zero, err
	}
	delaySec, delayErr := durationToSeconds(decision.Delay)
	if delayErr != nil {
		return zero, fmt.Errorf("durable: WaitForCondition %q: retry delay: %w", name, delayErr)
	}
	update := waitForConditionUpdate(ec, id, name, OperationActionRetry)
	update.Payload = aws.String(string(serialized))
	update.StepOptions = &StepOptions{
		NextAttemptDelaySeconds: aws.Int32(delaySec),
	}
	if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
		return zero, cerr
	}

	ec.blocked.Store(true)
	ec.suspend.commitPending(ec.abandon)
	return zero, errSuspendExecution
}

// runCheckFunc executes the check function with panic recovery. The trace
// is the stack trace of the failure as [runUserFunc] captures it, nil when
// the check succeeds or when capture is disabled.
func runCheckFunc[S any](ec *execContext, check func(StepContext, S) (S, error), state S) (S, []string, error) {
	return runUserFunc(ec, check, "durable: WaitForCondition check panicked", func() (S, error) {
		return check(&stepContext{Context: ec.Context, logger: ec.Logger()}, state)
	})
}

// waitForConditionUpdate assembles the shared fields of a
// wait-for-condition operation update. IDs are hashed to their wire form.
func waitForConditionUpdate(ec *execContext, id, name string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeStep,
		SubType: aws.String(operationSubTypeWaitForCondition),
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

// newWaitForConditionError builds the [WaitForConditionError] for a failed
// wait-for-condition from the failure record, on both the first invocation
// and on replay.
func newWaitForConditionError(name string, attempts int, rec errorRecord) *WaitForConditionError {
	return &WaitForConditionError{
		Name: name, Attempts: attempts,
		ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace,
		Err: rec.cause("", nil),
	}
}
