package durable

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// WaitOption configures a single [Wait] or [WaitAsync] operation.
//
// The interface is sealed: only this package can implement it. No option
// constructors exist yet. The parameter is present so that options can be
// added later without changing the signatures of Wait and WaitAsync.
type WaitOption interface {
	applyWait(*waitOptions)
}

// waitOptions holds the resolved configuration of one wait operation. It
// has no fields yet; see [WaitOption].
type waitOptions struct{}

// Wait pauses the execution for duration d. When the wait elapses while
// other work of the handler is still running, Wait returns in the same
// invocation. When nothing else can make progress, the invocation ends
// without consuming compute resources, and the execution resumes in a new
// invocation when the duration elapses. On replay a completed wait returns
// immediately. name identifies the wait for tracking and debugging; pass
// "" for an unnamed wait.
//
// The duration must be at least one second. For a shorter duration,
// including zero and any negative duration, Wait returns an error at the
// call: it records no operation and writes no checkpoint. A duration of one
// second or more is rounded up to a whole number of seconds.
func Wait(ctx Context, name string, d time.Duration, opts ...WaitOption) error {
	ec, ok := ctx.(*execContext)
	if !ok {
		return fmt.Errorf("durable: Wait %q: Context was not created by the SDK", name)
	}

	var options waitOptions
	for _, o := range opts {
		o.applyWait(&options)
	}
	if err := validateWaitDuration("Wait", name, d); err != nil {
		return err
	}

	id, err := ec.claimOperation(name, OperationSubTypeWait)
	if err != nil {
		return err
	}

	err = runWait(ec, id, name, d)
	ec.observeOutcome(id, err)
	return err
}

// WaitAsync is [Wait], except that the wait completes through the returned
// future, allowing other durable operations to proceed concurrently.
//
// The duration must be at least one second, as for [Wait]. For a shorter
// duration WaitAsync records no operation and returns a future that fails
// with the error.
//
// The future settles when the wait elapses, in the invocation that observes
// it. On invocation suspension, the returned future is settled with
// errSuspendExecution so goroutines blocked on [Future.Result] unwind.
func WaitAsync(ctx Context, name string, d time.Duration, opts ...WaitOption) *Future[Void] {
	ec, ok := ctx.(*execContext)
	if !ok {
		return newFailedFuture[Void](fmt.Errorf("durable: WaitAsync %q: Context was not created by the SDK", name))
	}

	var options waitOptions
	for _, o := range opts {
		o.applyWait(&options)
	}
	if err := validateWaitDuration("WaitAsync", name, d); err != nil {
		return newFailedFuture[Void](err)
	}

	id, err := ec.claimOperation(name, OperationSubTypeWait)
	if err != nil {
		return newFailedFuture[Void](err)
	}
	if ec.unfinishedInSucceededContext(ec.state.get(id)) {
		return newUnfinishedReplayFuture[Void](ec.suspend)
	}

	fut := newFuture[Void]().bind(ec.state, id)
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
		err := runWait(branch, id, name, d)
		fut.settle(Void{}, err)
	}()

	return fut
}

// minWaitDuration is the shortest duration [Wait] and [WaitAsync] accept.
// The service requires a wait of at least one whole second.
const minWaitDuration = time.Second

// validateWaitDuration returns an error when d is under [minWaitDuration].
// op is the public function name used in the message. The check runs before
// the operation claims its replay position, so a rejected wait records
// nothing.
func validateWaitDuration(op, name string, d time.Duration) error {
	if d < minWaitDuration {
		return fmt.Errorf("durable: %s %q: duration must be at least 1 second", op, name)
	}
	return nil
}

// runWait performs the wait logic for a previously-claimed operation ID.
// Separated from [Wait] so that both the blocking and async variants share
// the same core.
//
// After its START checkpoint, or when it is replayed while still STARTED,
// the wait parks the goroutine until a checkpoint response or a poll
// reports it SUCCEEDED, then returns. The first poll is sent at the wait's
// scheduled end. The invocation suspends instead when nothing else can make
// progress first.
//
// Operation lifecycle hooks: a wait dispatches at most one start and at most
// one end per invocation. A live wait dispatches a start after its START
// checkpoint. A wait replayed while still STARTED dispatches a replayed
// start. Either then dispatches a live end in the invocation that observes
// the wait SUCCEEDED, or no end when the invocation suspends first. A wait
// replayed as SUCCEEDED dispatches only a replayed end with the
// checkpointed timestamps: its start was dispatched by the invocation that
// recorded it.
func runWait(ec *execContext, id, name string, d time.Duration) error {
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeWait), OperationSubTypeWait, name); err != nil {
		return err
	}
	if ec.unfinishedInSucceededContext(op) {
		return ec.parkUnfinishedReplay(op, id, string(OperationTypeWait), OperationSubTypeWait, name)
	}
	if op != nil {
		switch op.status {
		case statusSucceeded:
			info := ec.operationHookInfo(id, name, string(OperationTypeWait), OperationSubTypeWait, true)
			info.StartTimestamp = op.startTimestamp
			info.EndTimestamp = op.endTimestamp
			dispatchOperationEnd(ec, info, PluginOperationSucceeded)
			return nil
		case statusStarted:
			// The timer has not fired. The start is replayed; the end
			// belongs to the invocation that observes the completion.
			info := ec.operationHookInfo(id, name, string(OperationTypeWait), OperationSubTypeWait, true)
			info.StartTimestamp = op.startTimestamp
			dispatchOperationStart(ec, info, PluginOperationStarted)
			return awaitWait(ec, id, name, info, checkpointedStartTime(op).Add(d))
		case statusPending, statusReady, statusFailed, statusCancelled, statusTimedOut, statusStopped:
			return fmt.Errorf("durable: wait %q: unexpected checkpointed status %s", name, op.status)
		}
	}

	waitSec, err := durationToSeconds(d)
	if err != nil {
		return fmt.Errorf("durable: Wait %q: %w", name, err)
	}

	update := OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    OperationTypeWait,
		SubType: aws.String(OperationSubTypeWait),
		Action:  OperationActionStart,
		WaitOptions: &WaitOptions{
			WaitSeconds: aws.Int32(waitSec),
		},
	}
	if name != "" {
		update.Name = aws.String(name)
	}
	if parent := ec.parentOperationID(); parent != "" {
		update.ParentId = aws.String(hashID(parent))
	}
	if err := ec.checkpointer.checkpoint(ec, []OperationUpdate{update}); err != nil {
		if errors.Is(err, errCheckpointTerminated) {
			return errSuspendExecution
		}
		return err
	}

	// The wait is recorded. Its start timestamp comes from the checkpoint
	// response when the response carried the record, else from the clock.
	info := ec.operationHookInfo(id, name, string(OperationTypeWait), OperationSubTypeWait, false)
	info.StartTimestamp = checkpointedStartTime(ec.state.get(id))
	dispatchOperationStart(ec, info, PluginOperationStarted)

	return awaitWait(ec, id, name, info, info.StartTimestamp.Add(time.Duration(waitSec)*time.Second))
}

// awaitWait parks the goroutine until the wait id is reported finished and
// dispatches its live end. fallbackEnd is the time the wait is expected to
// elapse when its record carries no scheduled end; the first poll is sent
// then. info is the hook info of the wait's start.
func awaitWait(ec *execContext, id, name string, info OperationHookInfo, fallbackEnd time.Time) error {
	op, err := ec.awaitOperation(id, terminalRecord, func(op *operation) time.Time {
		if op != nil && !op.scheduledEnd.IsZero() {
			return op.scheduledEnd
		}
		return fallbackEnd
	})
	if err != nil {
		return err
	}
	if op.status != statusSucceeded {
		return fmt.Errorf("durable: wait %q: unexpected checkpointed status %s", name, op.status)
	}
	info.IsReplay = false
	info.EndTimestamp = checkpointedEndTime(op)
	dispatchOperationEnd(ec, info, PluginOperationSucceeded)
	return nil
}

// checkpointedStartTime returns op's start timestamp when op exists and
// carries one, else the current time. Used for the start hook of a live
// operation right after its START checkpoint: the checkpoint response may
// or may not carry the operation record with its timestamp.
func checkpointedStartTime(op *operation) time.Time {
	if op != nil && !op.startTimestamp.IsZero() {
		return op.startTimestamp
	}
	return time.Now()
}
