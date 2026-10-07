package durable

import (
	"encoding/json"
	"errors"
	"sync"
)

// Void is the result type of operations that produce no value, such as
// [WaitAsync].
type Void struct{}

// Future is the result of an asynchronous durable operation. It settles
// exactly once, with either a value or an error, and can be read any number
// of times from any goroutine.
//
// The only ways to wait on a Future are [Future.Result] and the combinators
// [All], [AllSettled], [Any], [Race], and [Join]. Each of these checkpoints
// its outcome, so the value observed on first execution is the value
// observed on replay. A Future deliberately exposes no channel: a select
// over several futures picks whichever settles first in the current
// invocation, and that choice is not checkpointed. On replay every future
// may already be settled, so the select could pick a different case, and
// any code that branches on the winner would diverge from the first
// execution.
type Future[O any] struct {
	once  sync.Once
	done  chan struct{}
	value O
	err   error

	// s is the suspension signal the future is registered with, nil for
	// a future created settled. A goroutine that awaits an unsettled
	// registered future is parked on s until the future settles.
	s *suspendSignal

	// waiters are the parked goroutines awaiting the future. Guarded by
	// s.mu; settlement wakes them under it.
	waiters []*parkWaiter

	// preResult, when set, is called exactly once before the first await
	// of the future. A pending callback uses it to start watching the
	// callback only when the handler awaits it, so operations between
	// CreateCallback and Result (the submitter step of WaitForCallback)
	// run first. preResult must not block.
	preResultOnce sync.Once
	preResult     func()

	// newOutcome, when set, reports whether the future's outcome is one
	// the previous invocation did not have. Result applies it to the
	// context passed in; see execContext.receiveOutcome. It is set before
	// the future can settle, and read only after it settled. It is nil
	// for a future that is not bound to an operation, which never
	// switches a context to live.
	newOutcome func() bool
}

// Result blocks until the operation settles, then returns its outcome.
// Once the future has settled, Result returns immediately.
//
// An operation can settle in the invocation that awaits it: a wait that
// elapses, a callback that is completed, or an invoke that finishes while
// other work runs settles the future then. The invocation suspends only
// when no goroutine running handler code can make progress; Result then
// returns the suspension signal, which must be returned unchanged.
//
// ctx is the context of the code that reads the outcome: the context the
// calling goroutine runs on, which can differ from the context that
// started the operation, for example inside a [Go] branch. When the
// previous invocation did not have the outcome, the code after Result
// runs for the first time, so ctx stops replaying: its log records are
// written and [Context.IsReplaying] reports false. See [Context.Logger].
func (f *Future[O]) Result(ctx Context) (O, error) {
	value, err := f.result()
	if err != nil && errors.Is(err, errSuspendExecution) {
		// The suspension signal is not an outcome. It can settle the
		// future before the operation has bound newOutcome, so the
		// field is not read.
		return value, err
	}
	if ec, ok := ctx.(*execContext); ok && f.newOutcome != nil {
		ec.receiveOutcome(err, f.newOutcome)
	}
	return value, err
}

// result is [Future.Result] without a receiving context. The SDK reads
// futures with it where the outcome reaches user code through an
// operation of its own, such as a combinator.
func (f *Future[O]) result() (O, error) {
	f.activate()
	f.park()
	return f.value, f.err
}

// bind binds the future to the operation with the positional ID id, so
// that [Future.Result] reports a new outcome when state reports one for
// the operation. It must be called before the future can settle.
func (f *Future[O]) bind(state *executionState, id string) *Future[O] {
	f.newOutcome = func() bool { return state.newOutcome(id) }
	return f
}

// activate runs the pre-result hook exactly once.
func (f *Future[O]) activate() {
	if f.preResult != nil {
		f.preResultOnce.Do(f.preResult)
	}
}

// park blocks until the future settles, counting the calling goroutine as
// parked meanwhile.
func (f *Future[O]) park() {
	s := f.s
	if s == nil {
		<-f.done
		return
	}
	s.mu.Lock()
	if f.isDone() {
		s.mu.Unlock()
		return
	}
	w := s.newWaiterLocked(nil, false)
	f.waiters = append(f.waiters, w)
	s.mu.Unlock()
	s.maybeSuspend()
	<-w.ch
	<-f.done
}

// isDone reports whether the future has settled.
func (f *Future[O]) isDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// settled reports whether the future has settled, without blocking and
// without running the pre-result hook. It exists for internal tests that
// must observe a future's state without awaiting it.
func (f *Future[O]) settled() bool {
	return f.isDone()
}

// settle resolves the future with value and err. Only the first call takes
// effect; subsequent calls are no-ops. This guarantees settle-once
// semantics regardless of race between normal completion and suspension.
// The goroutines parked on the future are unparked before they can observe
// the outcome.
func (f *Future[O]) settle(value O, err error) {
	if s := f.s; s != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	f.settleLocked(value, err)
}

// settleLocked is settle for a caller that holds f.s.mu, or for a future
// with no signal.
func (f *Future[O]) settleLocked(value O, err error) {
	f.once.Do(func() {
		f.value = value
		f.err = err
		close(f.done)
		for _, w := range f.waiters {
			f.s.wakeLocked(w, nil)
		}
		f.waiters = nil
	})
}

// awaitAnySettled blocks until at least one of fs has settled. The calling
// goroutine is parked meanwhile, and the first settlement unparks it. It
// returns at once when one of fs has already settled or fs is empty.
//
// A future that is not registered with the suspension signal (one built
// by a test) is watched by a helper goroutine that unparks the caller when
// it settles.
func awaitAnySettled[O any](fs []*Future[O]) {
	var s *suspendSignal
	for _, f := range fs {
		if f.isDone() {
			return
		}
		if f.s != nil {
			s = f.s
		}
	}
	if len(fs) == 0 {
		return
	}
	stop := make(chan struct{})
	defer close(stop)
	if s == nil {
		ch := make(chan struct{})
		var once sync.Once
		for _, f := range fs {
			go func() {
				select {
				case <-f.done:
					once.Do(func() { close(ch) })
				case <-stop:
				}
			}()
		}
		<-ch
		return
	}

	s.mu.Lock()
	for _, f := range fs {
		if f.isDone() {
			s.mu.Unlock()
			return
		}
	}
	w := s.newWaiterLocked(nil, false)
	for _, f := range fs {
		if f.s != nil {
			f.waiters = append(f.waiters, w)
			continue
		}
		go func() {
			select {
			case <-f.done:
				s.mu.Lock()
				s.wakeLocked(w, nil)
				s.mu.Unlock()
			case <-stop:
			}
		}()
	}
	s.mu.Unlock()
	s.maybeSuspend()
	<-w.ch

	// Remove the waiter from the futures that have not settled, so they
	// do not hold it until they do.
	s.mu.Lock()
	for _, f := range fs {
		for i, fw := range f.waiters {
			if fw == w {
				f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
				break
			}
		}
	}
	s.mu.Unlock()
}

// newFuture creates an unsettled future.
func newFuture[O any]() *Future[O] {
	return &Future[O]{done: make(chan struct{})}
}

// newPendingCallbackFuture creates the future of a callback that is still
// pending (STARTED, or freshly checkpointed START). Nothing watches the
// callback until the future is first awaited, so operations between
// CreateCallback and Result (the submitter step of WaitForCallback) run
// first. The first await starts a branch that parks on the callback
// operation and settles the future with outcome(record) once a checkpoint
// response or a poll reports the callback finished, or with the
// suspension sentinel if the invocation suspends first.
//
// That branch does not hold the invocation at PENDING when the handler
// returns: a combinator may have returned another future's outcome while
// the callback is still pending.
func newPendingCallbackFuture[O any](ec *execContext, id string, outcome func(*operation) (O, error)) *Future[O] {
	f := newFuture[O]()
	registerFuture(ec.suspend, f)
	f.preResult = func() {
		tok := ec.suspend.registerBranchToken()
		go func() {
			defer tok.release()
			op, err := ec.suspend.awaitOperation(ec.state, id, ec.abandon, true, terminalRecord, noEndTime)
			if err != nil {
				var zero O
				f.settle(zero, err)
				return
			}
			f.settle(outcome(op))
		}()
	}
	return f
}

// newUnfinishedReplayFuture returns the future for an asynchronous
// operation that must not run because the enclosing child context's result
// is already recorded while the operation has no terminal checkpoint. The
// operation neither executes nor commits the invocation to PENDING; the
// future settles only if the invocation suspends, so awaiting goroutines
// unwind on suspension instead of hanging.
func newUnfinishedReplayFuture[O any](s *suspendSignal) *Future[O] {
	f := newFuture[O]()
	registerFuture(s, f)
	return f
}

// newSettledFuture returns a future pre-settled with value and err.
func newSettledFuture[O any](value O, err error) *Future[O] {
	f := &Future[O]{done: make(chan struct{}), value: value, err: err}
	// Use sync.Once to guard the close — safe against accidental
	// double-settle if this future is ever passed to settle().
	f.once.Do(func() { close(f.done) })
	return f
}

// newFailedFuture returns a future pre-settled with err.
func newFailedFuture[O any](err error) *Future[O] {
	var zero O
	return newSettledFuture(zero, err)
}

// Settled is the per-future outcome returned by [AllSettled].
type Settled[O any] struct {
	_ [0]func() // blocks unkeyed literals; keeps fields addable

	// Value is the future's result. It is the zero value when Err is
	// non-nil.
	Value O

	// Err is the future's error, or nil if the future succeeded.
	//
	// After a checkpoint round trip (AllSettled runs in a child context),
	// Err is rebuilt from the serialized outcome: an SDK error type is
	// rebuilt as that type with its [OperationError] fields, so
	// [errors.As] matches it and a timed-out callback still matches
	// [ErrCallbackTimedOut]; any other error is a stand-in whose Error()
	// is "<ErrorType>: <message>". Fields outside [OperationError], such
	// as [StepError.Attempts], are zero after the round trip.
	Err error
}

// settledJSON is the JSON-serializable form of Settled.
type settledJSON[O any] struct {
	Status string `json:"status"`
	Value  O      `json:"value,omitempty"`

	// Err is the error's message. Values written before the typed form
	// existed carry only this field.
	Err string `json:"error,omitempty"`

	// ErrorType is the wire ErrorType of the error itself: the SDK type
	// name for an SDK error, or the Go type name otherwise.
	ErrorType string `json:"errorType,omitempty"`

	// Operation carries the [OperationError] fields of an SDK error so the
	// concrete type can be rebuilt on deserialization.
	Operation *settledOperationJSON `json:"operation,omitempty"`
}

// settledOperationJSON is the serialized [OperationError] of a rejected
// Settled value.
type settledOperationJSON struct {
	Name       string   `json:"name,omitempty"`
	ErrorType  string   `json:"errorType,omitempty"`
	Message    string   `json:"message,omitempty"`
	ErrorData  string   `json:"errorData,omitempty"`
	StackTrace []string `json:"stackTrace,omitempty"`
}

// MarshalJSON serializes a Settled value. A rejected outcome records the
// error's message, its wire ErrorType, and, for an SDK operation error,
// the [OperationError] fields, so the SDK type is rebuilt on
// deserialization.
//
// The message is taken from [recordOf], not from Error(). A stand-in's
// Error() is "<ErrorType>: <message>"; storing that text would prefix the
// type name again on every further round trip. [recordOf] yields the
// stand-in's raw message, so a value that is serialized, deserialized, and
// serialized again keeps the same message.
func (s Settled[O]) MarshalJSON() ([]byte, error) {
	j := settledJSON[O]{Status: "fulfilled", Value: s.Value}
	if s.Err != nil {
		j.Status = "rejected"
		rec := recordOf(s.Err)
		j.Err = rec.message
		j.ErrorType = rec.errType
		var opErr *OperationError
		if errors.As(s.Err, &opErr) {
			j.Operation = &settledOperationJSON{
				Name: opErr.Name, ErrorType: opErr.ErrorType, Message: opErr.Message,
				ErrorData: opErr.ErrorData, StackTrace: opErr.StackTrace,
			}
		}
		var zero O
		j.Value = zero
	}
	return json.Marshal(j)
}

// UnmarshalJSON deserializes a Settled value. A rejected outcome that names
// an SDK error type is rebuilt as that type, so [errors.As] matches it and
// [errors.Is] matches its sentinel; fields outside [OperationError] are
// zero. An unknown name yields a stand-in whose Error() is
// "<ErrorType>: <message>". A value in the older message-only form yields
// a stand-in with ErrorType "Error".
func (s *Settled[O]) UnmarshalJSON(data []byte) error {
	var j settledJSON[O]
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	if j.Status == "rejected" {
		s.Err = settledError(j)
		var zero O
		s.Value = zero
	} else {
		s.Value = j.Value
		s.Err = nil
	}
	return nil
}

// settledError rebuilds the error of a rejected Settled value.
func settledError[O any](j settledJSON[O]) error {
	if j.ErrorType == "" {
		return &replayedError{errType: "Error", message: j.Err}
	}
	if j.Operation == nil {
		return &replayedError{errType: j.ErrorType, message: j.Err}
	}
	return reconstructSDKError(j.ErrorType, OperationError{
		Name: j.Operation.Name, ErrorType: j.Operation.ErrorType, Message: j.Operation.Message,
		ErrorData: j.Operation.ErrorData, StackTrace: j.Operation.StackTrace,
	}, nil)
}
