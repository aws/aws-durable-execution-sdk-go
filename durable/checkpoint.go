package durable

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const (
	// checkpointMaxAttempts is the maximum number of times a checkpoint
	// call is attempted on retryable errors before giving up.
	checkpointMaxAttempts = 3

	// checkpointBaseDelay is the initial delay before the first retry.
	checkpointBaseDelay = 100 * time.Millisecond

	// checkpointMaxDelay caps the exponential backoff.
	checkpointMaxDelay = 2 * time.Second
)

// errCheckpointTerminated is returned by the checkpointer after the
// invocation has committed to a PENDING response. Orphaned branches
// (durable.Go children still mid-flight when the handler unwinds) receive
// this error at their next checkpoint attempt and treat it as suspension:
// they settle their future with errSuspendExecution and release their
// branch token without recording any further state.
var errCheckpointTerminated = errors.New("durable: checkpoint refused: invocation terminated")

// checkpointer persists operation updates for one durable execution and
// tracks the rotating checkpoint token. It is safe for concurrent use:
// operation bodies running on multiple goroutines checkpoint through one
// checkpointer.
type checkpointer struct {
	client       ExecutionClient
	executionArn string

	mu    sync.Mutex
	token string

	// terminated is atomically set when the invocation commits to PENDING.
	// Checked without holding mu so that terminate() never blocks behind an
	// in-flight checkpoint API call. An in-flight checkpoint discovers
	// termination after its API call returns and refuses to rotate the token.
	terminated atomic.Bool

	// state is the shared execution state. When non-nil, the checkpointer
	// merges backend-returned operations into it so that subsequent reads
	// (e.g. CallbackId lookup after checkpointing START) reflect
	// backend-assigned fields. Set during invocation wiring.
	state *executionState
}

func newCheckpointer(client ExecutionClient, executionArn, initialToken string) *checkpointer {
	return &checkpointer{
		client:       client,
		executionArn: executionArn,
		token:        initialToken,
	}
}

// loadState fetches the complete checkpointed operation log for the
// execution, following pagination until exhausted.
//
// loadState must complete before any concurrent checkpoint calls begin: it
// runs during invocation setup, before operation goroutines exist, and
// reads the token without coordinating with in-flight rotation.
func (cp *checkpointer) loadState(ctx context.Context) (*executionState, error) {
	ops, err := cp.loadStateFrom(ctx, "")
	if err != nil {
		return nil, err
	}
	return newExecutionState(ops), nil
}

// loadStateFrom fetches operation-log pages starting at marker ("" for the
// first page), following pagination until exhausted. The same setup-time
// constraint as loadState applies.
func (cp *checkpointer) loadStateFrom(ctx context.Context, marker string) ([]*operation, error) {
	var ops []*operation
	next := marker
	for {
		out, err := cp.client.GetExecutionState(ctx, GetExecutionStateInput{
			ExecutionArn:    cp.executionArn,
			CheckpointToken: cp.currentToken(),
			Marker:          next,
		})
		if err != nil {
			return nil, fmt.Errorf("durable: load execution state: %w", err)
		}
		for _, op := range out.Operations {
			ops = append(ops, operationFromAPI(op))
		}
		if out.NextMarker == "" {
			break
		}
		next = out.NextMarker
	}
	return ops, nil
}

// terminate marks the checkpointer as terminated. All subsequent checkpoint
// calls return errCheckpointTerminated, and an in-flight checkpoint refuses
// to commit its result. Uses an atomic store so it never blocks behind a
// checkpoint holding mu.
func (cp *checkpointer) terminate() {
	cp.terminated.Store(true)
}

// checkpoint applies updates atomically and rotates the checkpoint token.
// The returned operations are the updated state from the backend response,
// which may include backend-assigned fields (e.g. CallbackId).
//
// On retryable failures (server faults, throttling, network errors),
// checkpoint retries up to [checkpointMaxAttempts] with exponential
// backoff. Non-retryable failures (client faults other than throttling)
// fail immediately. On any failure the token remains unchanged.
func (cp *checkpointer) checkpoint(ctx context.Context, updates []OperationUpdate) error {
	// Fast-path refusal: no lock required.
	if cp.terminated.Load() {
		return errCheckpointTerminated
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	var lastErr error
	for attempt := range checkpointMaxAttempts {
		// Re-check after acquiring the lock or between retries: terminate()
		// may have been called while we were waiting or sleeping.
		if cp.terminated.Load() {
			return errCheckpointTerminated
		}

		out, err := cp.client.Checkpoint(ctx, CheckpointInput{
			ExecutionArn:    cp.executionArn,
			CheckpointToken: cp.token,
			Updates:         updates,
		})
		if err != nil {
			classified := classifyCheckpointError(err)
			if !classified.Retryable() {
				return classified
			}
			lastErr = classified
			if attempt < checkpointMaxAttempts-1 {
				delay := checkpointBackoff(attempt)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
			continue
		}

		// The API call succeeded, but if termination was signaled while it
		// was in-flight, refuse to commit the result. The orphaned branch's
		// progress will be replayed on the next invocation.
		if cp.terminated.Load() {
			return errCheckpointTerminated
		}

		if out.CheckpointToken == "" {
			return errors.New("durable: checkpoint: backend returned no checkpoint token")
		}
		cp.token = out.CheckpointToken

		// Merge updated operations into the execution state so that
		// subsequent reads (e.g. reading CallbackId after START) see
		// backend-assigned fields.
		if cp.state != nil && out.NewExecutionState != nil {
			ops := make([]*operation, 0, len(out.NewExecutionState))
			for _, apiOp := range out.NewExecutionState {
				ops = append(ops, operationFromAPI(apiOp))
			}
			cp.state.merge(ops)
		}
		return nil
	}
	return lastErr
}

// checkpointBackoff computes the delay for the given retry attempt using
// exponential backoff capped at [checkpointMaxDelay].
func checkpointBackoff(attempt int) time.Duration {
	delay := time.Duration(float64(checkpointBaseDelay) * math.Pow(2, float64(attempt)))
	if delay > checkpointMaxDelay {
		delay = checkpointMaxDelay
	}
	return delay
}

func (cp *checkpointer) currentToken() string {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.token
}

// operationFromAPI converts a client operation record into the engine's
// internal record.
func operationFromAPI(op Operation) *operation {
	rec := &operation{
		id:      aws.ToString(op.Id),
		status:  operationStatus(op.Status),
		opType:  string(op.Type),
		subType: aws.ToString(op.SubType),
		name:    aws.ToString(op.Name),
	}
	if sd := op.StepDetails; sd != nil {
		rec.step = &stepDetails{
			attempt: int(sd.Attempt),
			result:  aws.ToString(sd.Result),
		}
		if sd.Error != nil {
			rec.step.errType = aws.ToString(sd.Error.ErrorType)
			rec.step.errMessage = aws.ToString(sd.Error.ErrorMessage)
		}
	}
	if id := op.ChainedInvokeDetails; id != nil {
		rec.invoke = &invokeDetails{result: aws.ToString(id.Result)}
		if id.Error != nil {
			rec.invoke.errType = aws.ToString(id.Error.ErrorType)
			rec.invoke.errMessage = aws.ToString(id.Error.ErrorMessage)
			rec.invoke.errData = aws.ToString(id.Error.ErrorData)
		}
	}
	if cd := op.ContextDetails; cd != nil {
		rec.childCtx = &contextDetails{result: aws.ToString(cd.Result)}
		if cd.ReplayChildren != nil && *cd.ReplayChildren {
			rec.childCtx.replayChildren = true
		}
		if cd.Error != nil {
			rec.childCtx.errType = aws.ToString(cd.Error.ErrorType)
			rec.childCtx.errMessage = aws.ToString(cd.Error.ErrorMessage)
			rec.childCtx.errData = aws.ToString(cd.Error.ErrorData)
		}
	}
	if cb := op.CallbackDetails; cb != nil {
		rec.callback = &callbackDetails{
			callbackID: aws.ToString(cb.CallbackId),
			result:     aws.ToString(cb.Result),
		}
		if cb.Error != nil {
			rec.callback.errType = aws.ToString(cb.Error.ErrorType)
			rec.callback.errMessage = aws.ToString(cb.Error.ErrorMessage)
		}
	}
	return rec
}
