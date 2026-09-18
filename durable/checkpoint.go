package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
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

	// checkpointMaxBatchUpdates caps how many operation updates one
	// checkpoint call carries, independent of their byte size.
	checkpointMaxBatchUpdates = 250

	// checkpointBatchOverheadBytes approximates the request envelope
	// (execution ARN, field names) that surrounds the updates. It is added
	// to the token length when measuring a batch against
	// [resultSizeLimitBytes].
	checkpointBatchOverheadBytes = 100
)

// errCheckpointTerminated is returned by the checkpointer once no further
// checkpoints can be made in this invocation: after the invocation has
// committed to a PENDING response, or after a checkpoint response arrived
// without a token. Orphaned branches (durable.Go children still mid-flight
// when the handler unwinds) receive this error at their next checkpoint
// attempt and treat it as suspension: they settle their future with
// errSuspendExecution and release their branch token without recording any
// further state.
var errCheckpointTerminated = errors.New("durable: checkpoint refused: invocation terminated")

// pendingCheckpoint is one caller's checkpoint request waiting in the queue.
type pendingCheckpoint struct {
	// ctx is the caller's context. A request whose context is done before
	// it is sent is dropped from the queue and settled with ctx.Err().
	ctx     context.Context
	updates []OperationUpdate

	// size is the approximate wire size of updates, measured once at
	// enqueue time so that batch assembly does not re-serialize.
	size int

	// done receives the request's outcome exactly once. It is buffered so
	// the flusher never blocks on a caller that has stopped waiting.
	done chan error
}

// checkpointer persists operation updates for one durable execution and
// tracks the rotating checkpoint token. It is safe for concurrent use:
// operation bodies running on multiple goroutines checkpoint through one
// checkpointer.
//
// Requests are queued and sent by a single flusher goroutine. The flusher
// takes as many queued requests as fit in one call (bounded by
// [resultSizeLimitBytes] and [checkpointMaxBatchUpdates]) and sends them
// together, so requests that arrive while a call is in flight are coalesced
// into the next call. Because one flusher sends calls one at a time, the
// token returned by call n is always the token sent with call n+1, and
// queued requests keep their arrival order within and across calls.
//
// mu guards only the queue, the flusher flag, and the token. It is never
// held during a client call or a retry sleep.
type checkpointer struct {
	client       ExecutionClient
	executionArn string

	mu       sync.Mutex
	token    string
	queue    []*pendingCheckpoint
	flushing bool

	// terminated is atomically set when the invocation commits to PENDING.
	// Checked without holding mu so that terminate() never blocks behind an
	// in-flight checkpoint API call. An in-flight checkpoint discovers
	// termination after its API call returns and refuses to rotate the token.
	terminated atomic.Bool

	// haltErr is set when the checkpointer terminates itself because the
	// service will accept no further checkpoints from this invocation. It
	// is the error the invocation must end with, whatever the handler
	// returns: errSuspendExecution when a checkpoint response carried no
	// token, or the stale-token *CheckpointError when the service rejected
	// the token as superseded. The first cause recorded wins. Guarded by mu.
	haltErr error

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
// checkpoint call.
func (cp *checkpointer) terminate() {
	cp.terminated.Store(true)
}

// halt terminates the checkpointer because the service will accept no
// further checkpoints from this invocation, and records end as the error
// the invocation must end with. The first recorded cause wins: a later
// failure cannot change how the invocation ends. halt runs before the
// requests in the failed call learn of the failure, so the handler always
// sees the cause when it reads haltCause after the user function returns.
func (cp *checkpointer) halt(end error) {
	cp.mu.Lock()
	if cp.haltErr == nil {
		cp.haltErr = end
	}
	cp.mu.Unlock()
	cp.terminate()
}

// haltCause returns the error the invocation must end with after the
// checkpointer halted itself, or nil when it has not. The handler reads it
// once the user function returns and lets it override the returned
// outcome: a result or ordinary error cannot be reported once the service
// has stopped accepting this invocation's checkpoints.
func (cp *checkpointer) haltCause() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.haltErr
}

// checkpoint applies updates atomically and rotates the checkpoint token.
// It queues the updates, starts the flusher if none is running, and waits
// for the call that carries them. Updates from other goroutines that are
// queued at the same time may travel in the same call.
//
// On retryable failures (server faults, throttling, network errors), the
// call carrying the updates is retried up to [checkpointMaxAttempts] with
// exponential backoff. Non-retryable failures (client faults other than
// throttling) fail immediately. On any failure the token remains unchanged
// and every request in the failed call receives the error.
//
// Two outcomes halt the checkpointer for the rest of the invocation. A
// stale-token rejection (see [CheckpointError]) returns the classified
// error and ends the invocation with it. A response without a token
// returns errCheckpointTerminated and ends the invocation with PENDING.
// Every later call returns errCheckpointTerminated.
//
// If ctx is done before the updates are sent, checkpoint returns ctx.Err()
// and the updates are dropped from the queue.
func (cp *checkpointer) checkpoint(ctx context.Context, updates []OperationUpdate) error {
	// Fast-path refusal: no lock required.
	if cp.terminated.Load() {
		return errCheckpointTerminated
	}

	p := &pendingCheckpoint{
		ctx:     ctx,
		updates: updates,
		size:    updatesWireSize(updates),
		done:    make(chan error, 1),
	}

	cp.mu.Lock()
	cp.queue = append(cp.queue, p)
	start := !cp.flushing
	if start {
		cp.flushing = true
	}
	cp.mu.Unlock()

	if start {
		go cp.flush()
	}

	select {
	case err := <-p.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flush is the single flusher. It drains the queue one batch at a time and
// exits when the queue is empty. Exactly one flush goroutine runs at a time:
// checkpoint starts one only when it observes flushing == false, and flush
// clears flushing under mu only when it observes an empty queue.
func (cp *checkpointer) flush() {
	for {
		cp.mu.Lock()
		if len(cp.queue) == 0 {
			cp.flushing = false
			cp.mu.Unlock()
			return
		}
		batch, token := cp.takeBatchLocked()
		cp.mu.Unlock()

		if len(batch) == 0 {
			// Every queued request had a done context and was settled
			// during assembly. Look for more work.
			continue
		}

		err := cp.send(batch, token)
		for _, p := range batch {
			p.done <- err
		}
	}
}

// takeBatchLocked removes the next batch from the head of the queue and
// returns it with the token it must be sent with. The caller holds mu.
//
// Requests whose context is already done are settled with ctx.Err() and
// skipped. The batch grows while the next request fits under
// [resultSizeLimitBytes] and [checkpointMaxBatchUpdates]. A request that
// would push the batch over either limit stays queued for the next call. A
// batch always carries at least one request, so a single request larger
// than the limit is still sent on its own.
func (cp *checkpointer) takeBatchLocked() ([]*pendingCheckpoint, string) {
	var (
		batch    []*pendingCheckpoint
		nUpdates int
		size     = len(cp.token) + checkpointBatchOverheadBytes
		consumed int
	)
	for consumed < len(cp.queue) {
		p := cp.queue[consumed]
		if err := p.ctx.Err(); err != nil {
			p.done <- err
			consumed++
			continue
		}
		if len(batch) > 0 &&
			(size+p.size > resultSizeLimitBytes || nUpdates+len(p.updates) > checkpointMaxBatchUpdates) {
			break
		}
		batch = append(batch, p)
		nUpdates += len(p.updates)
		size += p.size
		consumed++
	}
	cp.queue = slices.Delete(cp.queue, 0, consumed)
	return batch, cp.token
}

// send issues one checkpoint call carrying every request in batch, using
// token, and applies the result. It runs without holding mu. The call uses
// the first request's context: all requests in one invocation share the
// invocation context, and requests whose context was already done were
// removed during batch assembly.
func (cp *checkpointer) send(batch []*pendingCheckpoint, token string) error {
	ctx := batch[0].ctx
	var updates []OperationUpdate
	for _, p := range batch {
		updates = append(updates, p.updates...)
	}

	var lastErr error
	for attempt := range checkpointMaxAttempts {
		// Re-check between retries: terminate() may have been called while
		// we were sleeping.
		if cp.terminated.Load() {
			return errCheckpointTerminated
		}

		out, err := cp.client.Checkpoint(ctx, CheckpointInput{
			ExecutionArn:    cp.executionArn,
			CheckpointToken: token,
			Updates:         updates,
		})
		if err != nil {
			classified := classifyCheckpointError(err)
			if classified.isStaleToken() {
				// A newer invocation has superseded this one. The token
				// never becomes valid again, so a retry cannot succeed.
				// Halt: later checkpoint calls are refused, and the
				// invocation ends with this error, so the execution
				// continues in the invocation that holds the fresh token.
				cp.halt(classified)
				return classified
			}
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
			// A response without a token means the service will accept no
			// further checkpoints from this invocation. The execution is
			// not finished; this invocation just cannot make further
			// progress. Halt so the invocation ends with PENDING, as for
			// any other suspension: the requests in this call receive
			// errCheckpointTerminated, which every operation translates
			// into errSuspendExecution, and the handler ends the
			// invocation with errSuspendExecution even if user code
			// swallows that error. Abandoned work replays on the next
			// invocation.
			cp.halt(errSuspendExecution)
			return errCheckpointTerminated
		}

		cp.mu.Lock()
		cp.token = out.CheckpointToken
		// Merge updated operations into the execution state so that
		// subsequent reads (e.g. reading CallbackId after START) see
		// backend-assigned fields. Done under mu to keep the lock order
		// checkpointer.mu → executionState.mu that merge documents.
		if cp.state != nil && out.NewExecutionState != nil {
			ops := make([]*operation, 0, len(out.NewExecutionState))
			for _, apiOp := range out.NewExecutionState {
				ops = append(ops, operationFromAPI(apiOp))
			}
			cp.state.merge(ops)
		}
		cp.mu.Unlock()
		return nil
	}
	return lastErr
}

// updatesWireSize approximates the serialized size of updates in bytes.
// Payload strings dominate the size, so JSON length is a close estimate of
// the request body the updates will occupy.
func updatesWireSize(updates []OperationUpdate) int {
	if len(updates) == 0 {
		return 0
	}
	b, err := json.Marshal(updates)
	if err != nil {
		return 0
	}
	return len(b)
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
			rec.step.errData = aws.ToString(sd.Error.ErrorData)
			rec.step.stackTrace = sd.Error.StackTrace
		}
	}
	if id := op.ChainedInvokeDetails; id != nil {
		rec.invoke = &invokeDetails{result: aws.ToString(id.Result)}
		if id.Error != nil {
			rec.invoke.errType = aws.ToString(id.Error.ErrorType)
			rec.invoke.errMessage = aws.ToString(id.Error.ErrorMessage)
			rec.invoke.errData = aws.ToString(id.Error.ErrorData)
			rec.invoke.stackTrace = id.Error.StackTrace
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
			rec.childCtx.stackTrace = cd.Error.StackTrace
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
			rec.callback.errData = aws.ToString(cb.Error.ErrorData)
			rec.callback.stackTrace = cb.Error.StackTrace
		}
	}
	return rec
}
