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

	// Snapshot the serializer defaults on the owning goroutine: the owner
	// may call ConfigureSerdes before the goroutine below runs.
	defaults := ec.serdesDefaults()
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
			return nil
		case statusStarted:
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

	ec.blocked.Store(true)
	ec.suspend.commitPending(ec.abandon)
	return errSuspendExecution
}
