package durable

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// defaultConditionWaitStrategy returns the wait strategy used when
// [ConditionConfig].WaitStrategy is nil. It is the strategy the zero
// [WaitConfig] builds, so the default and a configured strategy share one
// implementation.
func defaultConditionWaitStrategy[S any]() WaitStrategy[S] {
	return newWaitStrategy(WaitConfig[S]{})
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
		serdes = ec.serdesDefaults().serdes
	}

	id, err := ec.claimOperation()
	if err != nil {
		return zero, err
	}

	return runWaitForCondition(ec, id, name, check, cfg, serdes)
}

// runWaitForCondition drives one wait-for-condition operation from its
// checkpointed status.
//
// Operation lifecycle hooks: the polling operation dispatches at most one
// start and at most one end per invocation, besides the attempt-level hooks
// each poll attempt dispatches. The start is dispatched once, by the
// invocation that runs the first attempt, before that attempt's hooks: live
// with status STARTED, or, when a previous invocation checkpointed the START
// and recorded no outcome, replayed with the checkpointed status. A later
// invocation that runs a further attempt dispatches no start. The end is
// dispatched when polling stops: live, with SUCCEEDED and the final state
// as Result or FAILED and the returned error, and Attempt set to the final
// attempt count; or replayed from a terminal checkpoint, with the
// checkpointed timestamps and attempt count. An invocation that suspends
// on a scheduled poll, or whose checkpoint the service refused because the
// invocation has terminated, dispatches no end.
func runWaitForCondition[S any](ec *execContext, id, name string, check func(StepContext, S) (S, error), cfg ConditionConfig[S], serdes Serdes) (S, error) {
	var zero S
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeStep), OperationSubTypeWaitForCondition, name); err != nil {
		return zero, err
	}
	if ec.unfinishedInSucceededContext(op) {
		return zero, ec.parkUnfinishedReplay(op, id, string(OperationTypeStep), OperationSubTypeWaitForCondition, name)
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
			// re-executing the check function. The replayed end is
			// dispatched first, so a failing Serdes does not suppress it.
			if op.step == nil {
				return zero, fmt.Errorf("durable: WaitForCondition %q: checkpointed %s operation has no step details", name, op.status)
			}
			info := waitForConditionReplayedInfo(ec, id, name, op)
			info.Result = op.step.result
			dispatchOperationEnd(ec, info, PluginOperationSucceeded)
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
			wfcErr := newWaitForConditionError(name, op.step.attempt, op.step.record())
			info := waitForConditionReplayedInfo(ec, id, name, op)
			info.Error = wfcErr
			dispatchOperationEnd(ec, info, PluginOperationFailed)
			return zero, wfcErr

		case statusPending:
			// A retry (continue) is scheduled and its timer has not
			// fired. Suspend; the backend re-invokes when the delay
			// elapses. The operation began in an earlier invocation
			// and has no outcome yet, so no hook is dispatched.
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

	// The operation's start belongs to its first attempt. op is nil for a
	// live first attempt; else a previous invocation checkpointed the START
	// and recorded no outcome, and the start is replayed with its status.
	info := ec.operationHookInfo(id, name, string(OperationTypeStep), OperationSubTypeWaitForCondition, op != nil)
	info.Attempt = attempt
	info.StartTimestamp = checkpointedStartTime(op)
	if attempt == 1 {
		status := PluginOperationStarted
		if op != nil {
			status = toPluginOperationStatus(op.status)
		}
		dispatchOperationStart(ec, info, status)
	}

	// The cycle counts as executing from its START checkpoint through the
	// checkpoint of its outcome; see runStep for the reason.
	result, serialized, err := func() (S, string, error) {
		ec.suspend.enterExecuting()
		defer ec.suspend.exitExecuting()
		return executeWaitForConditionAttempt(ec, id, name, check, cfg, serdes, op, attempt)
	}()

	// The end reports the outcome this invocation recorded. A suspension
	// on a scheduled poll is not an outcome.
	info.IsReplay = false
	if err == nil {
		info.EndTimestamp = checkpointedEndTime(ec.state.get(id))
		info.Result = serialized
		dispatchOperationEnd(ec, info, PluginOperationSucceeded)
	} else if !errors.Is(err, errSuspendExecution) {
		info.EndTimestamp = checkpointedEndTime(ec.state.get(id))
		info.Error = err
		dispatchOperationEnd(ec, info, PluginOperationFailed)
	}
	return result, err
}

// waitForConditionReplayedInfo returns the info of the replayed end of the
// wait-for-condition operation id, whose checkpoint op is terminal: the
// checkpointed timestamps and the attempt count recorded with the outcome.
func waitForConditionReplayedInfo(ec *execContext, id, name string, op *operation) OperationHookInfo {
	info := ec.operationHookInfo(id, name, string(OperationTypeStep), OperationSubTypeWaitForCondition, true)
	info.Attempt = op.step.attempt
	info.StartTimestamp = op.startTimestamp
	info.EndTimestamp = op.endTimestamp
	return info
}

// executeWaitForConditionAttempt runs one cycle: checkpoint START (if not
// already started), deserialize current state, call check, consult the
// wait strategy, and checkpoint the outcome. When the condition is met it
// returns the final state and its serialized form as checkpointed; the
// serialized form is empty on every other outcome.
func executeWaitForConditionAttempt[S any](ec *execContext, id, name string, check func(StepContext, S) (S, error), cfg ConditionConfig[S], serdes Serdes, op *operation, attempt int) (S, string, error) {
	var zero S

	// Checkpoint START if this is a new attempt. If status is already
	// STARTED the previous invocation already checkpointed it.
	if op == nil || op.status != statusStarted {
		update := waitForConditionUpdate(ec, id, name, OperationActionStart)
		if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
			return zero, "", suspendIfTerminated(err)
		}
	}

	// Determine the current state: deserialize from the checkpoint
	// (resuming after a continue) or use the initial state.
	var currentState S
	if op != nil && op.step != nil && op.step.result != "" {
		if err := serdes.Unmarshal(ec.Context, ec.serdesCtx(id), []byte(op.step.result), &currentState); err != nil {
			// The checkpointed state cannot be reconstructed; the
			// operation cannot continue with a consistent view of it.
			return zero, "", newSerdesError(name, serdesDirectionUnmarshal, err)
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
			SubType:        OperationSubTypeWaitForCondition,
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

	wrappedResult, wrappedErr := wrapChain(ec.pluginDispatcher, ec,
		func(p *Plugin) wrapHook {
			if p.WrapOperationAttemptFn == nil {
				return nil
			}
			return func(ctx context.Context, innerFn wrapBody) (any, error) {
				return p.WrapOperationAttemptFn(ctx, attemptInfo, innerFn)
			}
		},
		func(ctx context.Context) (any, error) {
			s, trace, e := runCheckFunc(ctx, ec, id, name, check, currentState, attempt)
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
			return zero, "", suspendIfTerminated(cerr)
		}
		return zero, "", newWaitForConditionError(name, attempt, rec)
	}

	// Serialize the new state for checkpointing.
	serialized, err := serdes.Marshal(ec.Context, ec.serdesCtx(id), newState)
	if err != nil {
		return zero, "", newSerdesError(name, serdesDirectionMarshal, err)
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
		return zero, "", newSerdesError(name, serdesDirectionUnmarshal, err)
	}

	// Consult the wait strategy with the deserialized state, substituting
	// the documented default strategy when none is configured.
	strategy := cfg.WaitStrategy
	if strategy == nil {
		strategy = defaultConditionWaitStrategy[S]()
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
				return zero, "", suspendIfTerminated(cerr)
			}
			return zero, "", newWaitForConditionError(name, attempt, rec)
		}

		// Condition met: checkpoint terminal SUCCEED with the final state.
		if err := checkResultSize(serialized, name); err != nil {
			return zero, "", err
		}
		update := waitForConditionUpdate(ec, id, name, OperationActionSucceed)
		update.Payload = aws.String(string(serialized))
		if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
			return zero, "", suspendIfTerminated(cerr)
		}
		return deserialized, string(serialized), nil
	}

	// Condition not met: checkpoint RETRY with the intermediate state and
	// the strategy's delay, then suspend. The intermediate state is a
	// checkpoint payload like the final result, so the same size limit
	// applies to it.
	if err := checkResultSize(serialized, name); err != nil {
		return zero, "", err
	}
	delaySec, delayErr := durationToSeconds(decision.Delay)
	if delayErr != nil {
		return zero, "", fmt.Errorf("durable: WaitForCondition %q: retry delay: %w", name, delayErr)
	}
	update := waitForConditionUpdate(ec, id, name, OperationActionRetry)
	update.Payload = aws.String(string(serialized))
	update.StepOptions = &StepOptions{
		NextAttemptDelaySeconds: aws.Int32(delaySec),
	}
	if cerr := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); cerr != nil {
		return zero, "", suspendIfTerminated(cerr)
	}

	ec.blocked.Store(true)
	ec.suspend.commitPending(ec.abandon)
	return zero, "", errSuspendExecution
}

// runCheckFunc executes the check function with panic recovery. ctx is the
// parent of the check's [StepContext]: ec's context, or the one the wrap
// hooks supplied. The check observes attempt, the 1-based poll attempt
// number, through [StepContext.Attempt]. The trace is the stack trace of
// the failure as [runUserFunc] captures it, nil when the check succeeds or
// when capture is disabled.
func runCheckFunc[S any](ctx context.Context, ec *execContext, id, name string, check func(StepContext, S) (S, error), state S, attempt int) (S, []string, error) {
	return runUserFunc(ec, check, "durable: WaitForCondition check panicked", func() (S, error) {
		return check(&stepContext{Context: ctx, logger: ec.operationLogger(id, name, attempt), attempt: attempt}, state)
	})
}

// suspendIfTerminated translates a checkpoint failure into the error the
// wait-for-condition attempt returns. errCheckpointTerminated means the
// service accepts no further checkpoints from this invocation. The
// operation has no outcome; the invocation suspends and the next one
// replays it. So the attempt returns errSuspendExecution, as every other
// operation does. Any other checkpoint error is returned unchanged.
func suspendIfTerminated(err error) error {
	if errors.Is(err, errCheckpointTerminated) {
		return errSuspendExecution
	}
	return err
}

// waitForConditionUpdate assembles the shared fields of a
// wait-for-condition operation update. IDs are hashed to their wire form.
func waitForConditionUpdate(ec *execContext, id, name string, action OperationAction) OperationUpdate {
	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeStep,
		SubType: aws.String(OperationSubTypeWaitForCondition),
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
