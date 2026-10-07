package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const (
	// checkpointBatchLimitBytes caps the byte size of one checkpoint call
	// (750 KiB). The flusher stops adding requests to a batch before the
	// batch would exceed it. It is not a payload limit: a single request
	// larger than this is still sent on its own, and the service applies
	// its own limit to each kind of payload.
	checkpointBatchLimitBytes = 750 * 1024

	// checkpointMaxBatchUpdates caps how many operation updates one
	// checkpoint call carries, independent of their byte size.
	checkpointMaxBatchUpdates = 250

	// checkpointBatchOverheadBytes approximates the request envelope
	// (execution ARN, field names) that surrounds the updates. It is added
	// to the token length when measuring a batch against
	// [checkpointBatchLimitBytes].
	checkpointBatchOverheadBytes = 100
)

// errCheckpointTerminated is returned by the checkpointer once no further
// checkpoints can be made in this invocation: after the handler's outcome
// is decided (suspension, success, error, or panic), or after a checkpoint
// response arrived without a token. Orphaned branches (durable.Go children
// still mid-flight when the handler unwinds) receive this error at their
// next checkpoint attempt and treat it as suspension: they settle their
// future with errSuspendExecution and release their branch token without
// recording any further state.
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

	// final marks the invocation's own final write (see checkpointFinal).
	// Termination refuses every other request; a final request is sent.
	final bool
}

// checkpointer persists operation updates for one durable execution and
// tracks the rotating checkpoint token. It is safe for concurrent use:
// operation bodies running on multiple goroutines checkpoint through one
// checkpointer.
//
// Requests are queued and sent by a single flusher goroutine. The flusher
// takes as many queued requests as fit in one call (bounded by
// [checkpointBatchLimitBytes] and [checkpointMaxBatchUpdates]) and sends them
// together, so requests that arrive while a call is in flight are coalesced
// into the next call. Because one flusher sends calls one at a time, the
// token returned by call n is always the token sent with call n+1, and
// queued requests keep their arrival order within and across calls.
//
// mu guards only the queue, the flusher flag, and the token. It is never
// held during a client call.
type checkpointer struct {
	client       ExecutionClient
	executionArn string

	mu       sync.Mutex
	token    string
	queue    []*pendingCheckpoint
	flushing bool

	// terminated is atomically set when the handler's outcome is decided,
	// on every exit. Checked without holding mu so that terminate() never
	// blocks behind an in-flight checkpoint API call. An in-flight checkpoint
	// discovers termination after its API call returns: the token the call
	// rotated to is kept, because the service holds it, but the requests in
	// the call are refused. Only the invocation's final write (see
	// checkpointFinal) is sent after termination.
	terminated atomic.Bool

	// haltErr is set when the checkpointer terminates itself because the
	// service will accept no further checkpoints from this invocation. It
	// is the error the invocation must end with, whatever the handler
	// returns: errSuspendExecution when a checkpoint response carried no
	// token, the stale-token *CheckpointError when the service rejected
	// the token as superseded, or an execution-scoped *CheckpointError when
	// the service rejected a call in a way that fails the execution. The
	// first cause recorded wins. Guarded by mu.
	haltErr error

	// state is the shared execution state. When non-nil, the checkpointer
	// merges backend-returned operations into it so that subsequent reads
	// (e.g. CallbackId lookup after checkpointing START) reflect
	// backend-assigned fields. Set during invocation wiring.
	state *executionState

	// outstanding counts the requests queued or in flight: incremented
	// when a request is queued and decremented when its outcome is
	// delivered. The invocation does not suspend while it is above zero.
	outstanding atomic.Int64

	// idleMu guards idleWaiters, the channels awaitIdle waits on. They
	// are closed when outstanding drops to zero. idleMu is separate from
	// mu because requests are delivered while mu is held.
	idleMu      sync.Mutex
	idleWaiters []chan struct{}

	// onTokenWithdrawn, when non-nil, is called once when a response
	// without a token halts the checkpointer. Set during invocation
	// wiring to log the halt.
	onTokenWithdrawn func()
	// suspend, when non-nil, is told about every queued and settled
	// request and is given the records of every checkpoint response, so
	// that a response reporting an awaited operation finished resumes the
	// goroutine parked on it. Set during invocation wiring.
	suspend *suspendSignal
}

// busy reports whether a checkpoint request is queued or in flight.
func (cp *checkpointer) busy() bool {
	return cp.outstanding.Load() > 0
}

// deliver hands a request its outcome and, when no request remains queued
// or in flight, lets the suspension conditions be checked again.
func (cp *checkpointer) deliver(p *pendingCheckpoint, err error) {
	p.done <- err
	if cp.outstanding.Add(-1) == 0 {
		cp.idleMu.Lock()
		for _, ch := range cp.idleWaiters {
			close(ch)
		}
		cp.idleWaiters = nil
		cp.idleMu.Unlock()
		if cp.suspend != nil {
			if len(p.updates) > 0 {
				cp.suspend.touch()
			}
			cp.suspend.maybeSuspend()
		}
	}
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
			// A client that states the failure's scope is reported
			// with its own type and message, so a FAILED response
			// records the client error rather than this wrapper.
			var clientErr *ClientError
			if errors.As(err, &clientErr) {
				return nil, clientErr
			}
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
// calls return errCheckpointTerminated, and the requests in an in-flight
// checkpoint receive that error once its call returns. The invocation's own
// final write through checkpointFinal is still sent. Uses an atomic store so
// it never blocks behind a checkpoint call.
func (cp *checkpointer) terminate() {
	cp.terminated.Store(true)
}

// halt terminates the checkpointer because the service will accept no
// further checkpoints from this invocation, or because a serdes reported a
// transient failure and the invocation must end without recording more
// state. It records end as the error the invocation must end with. The first recorded cause wins: a later
// failure cannot change how the invocation ends. halt runs before the
// requests in the failed call learn of the failure, so the handler always
// sees the cause when it reads haltCause after the user function returns.
//
// halt reports whether this call recorded the cause, that is, whether no
// cause was recorded before it.
func (cp *checkpointer) halt(end error) bool {
	cp.mu.Lock()
	recorded := cp.haltErr == nil
	if recorded {
		cp.haltErr = end
	}
	cp.mu.Unlock()
	cp.terminate()
	return recorded
}

// awaitIdle blocks until no checkpoint request is queued or in flight, or
// until ctx is done. The handler calls it after terminating the
// checkpointer, so the queue only shrinks: queued branch requests are
// refused without a call, and a call already in flight delivers its
// outcome when its response arrives.
func (cp *checkpointer) awaitIdle(ctx context.Context) {
	cp.idleMu.Lock()
	if cp.outstanding.Load() == 0 {
		cp.idleMu.Unlock()
		return
	}
	ch := make(chan struct{})
	cp.idleWaiters = append(cp.idleWaiters, ch)
	cp.idleMu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

// carriesExecutionUpdate reports whether updates include an update of the
// execution operation itself. Only the invocation's terminal update of the
// execution's result does.
func carriesExecutionUpdate(updates []OperationUpdate) bool {
	for _, u := range updates {
		if u.Type == OperationTypeExecution {
			return true
		}
	}
	return false
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
// The SDK makes one Checkpoint call per batch and does not retry it. The
// default client's AWS standard retryer is the only retry: it retries
// server faults, throttling, and connection errors before the call
// returns. On any failure the token remains unchanged and every request in
// the failed call receives the error. An invocation-scoped failure ends
// the invocation, and the service invokes the execution again.
//
// Three outcomes halt the checkpointer for the rest of the invocation. A
// stale-token rejection (see [CheckpointError]) returns the classified
// error and ends the invocation with it. An execution-scoped failure
// returns the classified error and makes the invocation respond FAILED
// with it. A response without a token to a call that does not carry the
// execution's terminal update returns errCheckpointTerminated and ends the
// invocation with PENDING. A response without a token to a call that
// carries the execution's terminal update means the execution finished;
// it succeeds and halts nothing.
// Every later call returns errCheckpointTerminated.
//
// If ctx is done before the updates are sent, checkpoint returns ctx.Err()
// and the updates are dropped from the queue.
func (cp *checkpointer) checkpoint(ctx context.Context, updates []OperationUpdate) error {
	// Fast-path refusal: no lock required.
	if cp.terminated.Load() {
		return errCheckpointTerminated
	}
	return cp.enqueue(ctx, updates, false)
}

// checkpointFinal records the invocation's own final write: the result of a
// handler that returned successfully, when that result is too large for the
// response and is persisted through a checkpoint instead. The handler's
// outcome is decided before this write, so the checkpointer is already
// terminated when it runs. Termination refuses branch checkpoints; it does
// not refuse this write, which belongs to the invocation itself.
//
// The write travels through the same flusher as every other request, so it
// is sent after any call already in flight and with the token that call
// rotated to. It is refused only when the service has stopped accepting
// this invocation's checkpoints: after an earlier response without a token
// it returns errCheckpointTerminated, and after a stale-token rejection or
// an execution-scoped failure it returns that error, exactly as checkpoint
// would. A response without a token to the write itself means the
// execution finished, so the write succeeds.
//
// Only the invocation goroutine calls checkpointFinal, at most once, after
// the handler's outcome is decided. Branch checkpoints never use it.
func (cp *checkpointer) checkpointFinal(ctx context.Context, updates []OperationUpdate) error {
	return cp.enqueue(ctx, updates, true)
}

// enqueue queues one request, starts the flusher if none is running, and
// waits for the request's outcome or for ctx to be done.
func (cp *checkpointer) enqueue(ctx context.Context, updates []OperationUpdate, final bool) error {
	p := &pendingCheckpoint{
		ctx:     ctx,
		updates: updates,
		size:    updatesWireSize(updates),
		done:    make(chan error, 1),
		final:   final,
	}

	// A poll (a request with no updates) changes nothing the suspension
	// conditions depend on unless its response reports a change, which
	// the watches account for, so only a request with updates advances
	// the generation. Any request, poll or not, holds the suspension
	// while it is queued or in flight.
	cp.outstanding.Add(1)
	if cp.suspend != nil && len(updates) > 0 {
		cp.suspend.touch()
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
			cp.deliver(p, err)
		}
	}
}

// takeBatchLocked removes the next batch from the head of the queue and
// returns it with the token it must be sent with. The caller holds mu.
//
// Requests whose context is already done are settled with ctx.Err() and
// skipped. Once the checkpointer is terminated, requests other than the
// invocation's final write are settled with errCheckpointTerminated and
// skipped. The batch grows while the next request fits under
// [checkpointBatchLimitBytes] and [checkpointMaxBatchUpdates]. A request that
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
	terminated := cp.terminated.Load()
	for consumed < len(cp.queue) {
		p := cp.queue[consumed]
		if err := p.ctx.Err(); err != nil {
			cp.deliver(p, err)
			consumed++
			continue
		}
		if terminated && !p.final {
			// Termination refuses queued branch requests without a call.
			// A final request is always queued after terminate(), so a
			// batch that carries one carries no branch request.
			cp.deliver(p, errCheckpointTerminated)
			consumed++
			continue
		}
		if len(batch) > 0 &&
			(size+p.size > checkpointBatchLimitBytes || nUpdates+len(p.updates) > checkpointMaxBatchUpdates) {
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
	// A batch never mixes the invocation's final write with branch
	// requests (see takeBatchLocked), so the first request speaks for all.
	final := batch[0].final
	var updates []OperationUpdate
	for _, p := range batch {
		updates = append(updates, p.updates...)
	}

	if err := cp.refusal(final); err != nil {
		return err
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
			// never becomes valid again. Halt: later checkpoint calls
			// are refused, and the invocation ends with this error, so
			// the execution continues in the invocation that holds the
			// fresh token.
			cp.halt(classified)
			return classified
		}
		if classified.Scope() == ErrorScopeExecution {
			// The service rejected the call in a way no retry can fix,
			// and the operations it carried were not recorded. Halt:
			// later checkpoint calls are refused, and the invocation
			// responds FAILED with this error even if handler code
			// catches it and returns a value.
			cp.halt(classified)
			return classified
		}
		// An invocation-scoped failure. The client's own retryer has
		// already retried it, so the SDK adds no retry. The error ends
		// the invocation, and the service invokes the execution again.
		return classified
	}

	if out.CheckpointToken == "" {
		// A response without a token means the service will accept no
		// further checkpoints from this invocation. What that means
		// depends on what the call carried.
		if carriesExecutionUpdate(updates) {
			// The call carried the execution's own terminal update,
			// so the execution has reached its terminal state. No
			// further checkpoint is needed: the call succeeded, and
			// the invocation reports the terminal outcome.
			return nil
		}
		// Any other call, a poll included: the execution is not
		// finished, and this invocation cannot make further progress.
		// Halt so the invocation ends with PENDING, as for any other
		// suspension: the requests in this call receive
		// errCheckpointTerminated, which every operation translates
		// into errSuspendExecution, and the handler ends the
		// invocation with errSuspendExecution even if user code
		// swallows that error. The response's execution state is not
		// applied. Abandoned work replays on the next invocation.
		if cp.halt(errSuspendExecution) && cp.onTokenWithdrawn != nil {
			cp.onTokenWithdrawn()
		}
		return errCheckpointTerminated
	}

	cp.mu.Lock()
	cp.token = out.CheckpointToken
	// Merge updated operations into the execution state so that
	// subsequent reads (e.g. reading CallbackId after START) see
	// backend-assigned fields. Done under mu to keep the lock order
	// checkpointer.mu → executionState.mu that merge documents.
	var merged []*operation
	if cp.state != nil && out.NewExecutionState != nil {
		merged = make([]*operation, 0, len(out.NewExecutionState))
		for _, apiOp := range out.NewExecutionState {
			merged = append(merged, operationFromAPI(apiOp))
		}
		cp.state.merge(merged)
	}
	cp.mu.Unlock()
	// The records are matched against the watches after the merge,
	// so a goroutine that starts awaiting an operation in between
	// reads the merged record itself. A goroutine resumed here is
	// unparked before the requests of this call learn their outcome.
	if cp.suspend != nil && len(merged) > 0 {
		cp.suspend.onStateMerged(merged)
	}

	// The API call succeeded. The service rotated the token, so the
	// local token follows it above whatever happened meanwhile: the
	// invocation's final write, if any, must carry the token the
	// service now expects. If termination was signaled while the call
	// was in flight, the branch requests it carried are refused all
	// the same: the branch treats the refusal as suspension and
	// records nothing further in this invocation.
	if !final && cp.terminated.Load() {
		return errCheckpointTerminated
	}
	return nil
}

// refusal reports whether a request must be refused before its call is
// sent, and with what error. A branch request is refused once the
// checkpointer is terminated. The invocation's final write is refused only
// once the checkpointer has halted because the service stopped accepting
// this invocation's checkpoints: with errCheckpointTerminated after a
// response without a token, so the invocation responds PENDING, or with the
// recorded stale-token rejection or execution-scoped failure, so the
// invocation ends with it.
func (cp *checkpointer) refusal(final bool) error {
	if !cp.terminated.Load() {
		return nil
	}
	if !final {
		return errCheckpointTerminated
	}
	halt := cp.haltCause()
	switch {
	case halt == nil:
		return nil
	case errors.Is(halt, errSuspendExecution):
		return errCheckpointTerminated
	default:
		return halt
	}
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
	rec.setTimestamps(op.StartTimestamp, op.EndTimestamp)
	if sd := op.StepDetails; sd != nil {
		rec.step = &stepDetails{
			attempt: int(sd.Attempt),
			result:  aws.ToString(sd.Result),
		}
		if sd.NextAttemptTimestamp != nil {
			rec.step.nextAttempt = *sd.NextAttemptTimestamp
		}
		if sd.Error != nil {
			rec.step.errType = aws.ToString(sd.Error.ErrorType)
			rec.step.errMessage = aws.ToString(sd.Error.ErrorMessage)
			rec.step.errData = aws.ToString(sd.Error.ErrorData)
			rec.step.stackTrace = sd.Error.StackTrace
		}
	}
	if wd := op.WaitDetails; wd != nil && wd.ScheduledEndTimestamp != nil {
		rec.scheduledEnd = *wd.ScheduledEndTimestamp
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
