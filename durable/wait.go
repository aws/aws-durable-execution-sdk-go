package durable

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// operationSubTypeWait is the wire subtype for wait operations.
const operationSubTypeWait = "Wait"

// Wait suspends the execution for duration d without consuming compute
// resources: the invocation ends and the backend resumes the execution in a
// new invocation when the duration elapses. On replay a completed wait
// returns immediately. name identifies the wait for tracking and debugging;
// pass "" for an unnamed wait.
//
// The duration is rounded up to a whole number of seconds.
func Wait(ctx Context, name string, d time.Duration) error {
	ec, ok := ctx.(*execContext)
	if !ok {
		return fmt.Errorf("durable: Wait %q: Context was not created by the SDK", name)
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
func WaitAsync(ctx Context, name string, d time.Duration) *Future[Void] {
	ec, ok := ctx.(*execContext)
	if !ok {
		return newFailedFuture[Void](fmt.Errorf("durable: WaitAsync %q: Context was not created by the SDK", name))
	}

	id, err := ec.claimOperation()
	if err != nil {
		return newFailedFuture[Void](err)
	}

	fut := newFuture[Void]()
	registerFuture(ec.suspend, fut)

	tok := ec.suspend.registerBranchToken()
	go func() {
		defer tok.release()
		branch := ec.branch(currentGoroutineOwner())
		branch.branchTok = tok
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
	if err := validateReplayConsistency(op, string(types.OperationTypeWait), operationSubTypeWait, name); err != nil {
		return err
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

	update := types.OperationUpdate{
		Id:      aws.String(hashID(id)),
		Type:    types.OperationTypeWait,
		SubType: aws.String(operationSubTypeWait),
		Action:  types.OperationActionStart,
		WaitOptions: &types.WaitOptions{
			WaitSeconds: aws.Int32(waitSec),
		},
	}
	if name != "" {
		update.Name = aws.String(name)
	}
	if parent := ec.ids.prefix; parent != "" {
		update.ParentId = aws.String(hashID(parent))
	}
	if err := ec.checkpointer.checkpoint(ec, []types.OperationUpdate{update}); err != nil {
		return err
	}

	ec.blocked.Store(true)
	ec.suspend.commitPending(ec.abandon)
	return errSuspendExecution
}
