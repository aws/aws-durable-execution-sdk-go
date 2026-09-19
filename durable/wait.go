package durable

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// operationSubTypeWait is the wire subtype for wait operations.
const operationSubTypeWait = "Wait"

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

// Wait suspends the execution for duration d without consuming compute
// resources: the invocation ends and the execution resumes in a
// new invocation when the duration elapses. On replay a completed wait
// returns immediately. name identifies the wait for tracking and debugging;
// pass "" for an unnamed wait.
//
// The duration is rounded up to a whole number of seconds.
func Wait(ctx Context, name string, d time.Duration, opts ...WaitOption) error {
	ec, ok := ctx.(*execContext)
	if !ok {
		return fmt.Errorf("durable: Wait %q: Context was not created by the SDK", name)
	}

	var options waitOptions
	for _, o := range opts {
		o.applyWait(&options)
	}

	id, err := ec.claimOperation()
	if err != nil {
		return err
	}

	return runWait(ec, id, name, d)
}

// WaitAsync is [Wait], except that the wait completes through the returned
// future, allowing other durable operations to proceed concurrently.
//
// On invocation suspension, the returned future is settled with
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

	id, err := ec.claimOperation()
	if err != nil {
		return newFailedFuture[Void](err)
	}
	if ec.unfinishedInSucceededContext(ec.state.get(id)) {
		return newUnfinishedReplayFuture[Void](ec.suspend)
	}

	fut := newFuture[Void]()
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

// runWait performs the wait logic for a previously-claimed operation ID.
// Separated from [Wait] so that both the blocking and async variants share
// the same core.
//
// Operation lifecycle hooks: a wait dispatches at most one start and at most
// one end per invocation. A live wait dispatches a start after its START
// checkpoint and then suspends, with no end. A wait replayed while still
// STARTED dispatches a replayed start and suspends. A wait replayed as
// SUCCEEDED dispatches only a replayed end with the checkpointed timestamps:
// its start was dispatched by the invocation that recorded it.
func runWait(ec *execContext, id, name string, d time.Duration) error {
	op := ec.state.get(id)
	if err := validateReplayConsistency(op, string(OperationTypeWait), operationSubTypeWait, name); err != nil {
		return err
	}
	if ec.unfinishedInSucceededContext(op) {
		return ec.parkUnfinishedReplay(op, id, string(OperationTypeWait), operationSubTypeWait, name)
	}
	if op != nil {
		switch op.status {
		case statusSucceeded:
			info := ec.operationHookInfo(id, name, string(OperationTypeWait), operationSubTypeWait, true)
			info.StartTimestamp = op.startTimestamp
			info.EndTimestamp = op.endTimestamp
			dispatchOperationEnd(ec, info, PluginOperationSucceeded)
			return nil
		case statusStarted:
			// The timer has not fired. The start is replayed; the end
			// belongs to the invocation that observes the completion.
			info := ec.operationHookInfo(id, name, string(OperationTypeWait), operationSubTypeWait, true)
			info.StartTimestamp = op.startTimestamp
			dispatchOperationStart(ec, info, PluginOperationStarted)
			ec.blocked.Store(true)
			ec.suspend.commitPending(ec.abandon)
			return errSuspendExecution
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
		SubType: aws.String(operationSubTypeWait),
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
	info := ec.operationHookInfo(id, name, string(OperationTypeWait), operationSubTypeWait, false)
	info.StartTimestamp = checkpointedStartTime(ec.state.get(id))
	dispatchOperationStart(ec, info, PluginOperationStarted)

	ec.blocked.Store(true)
	ec.suspend.commitPending(ec.abandon)
	return errSuspendExecution
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
