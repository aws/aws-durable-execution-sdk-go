package durable

import (
	"encoding/json"
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
// [All], [AllSettled], [Any], and [Race]. Each of these checkpoints its
// outcome, so the value observed on first execution is the value observed
// on replay. A Future deliberately exposes no channel: a select over
// several futures picks whichever settles first in the current invocation,
// and that choice is not checkpointed. On replay every future may already
// be settled, so the select could pick a different case, and any code that
// branches on the winner would diverge from the first execution.
type Future[O any] struct {
	once  sync.Once
	done  chan struct{}
	value O
	err   error

	// preResult, when set, is called exactly once before the first
	// Result() blocks. It enables deferred suspension: a pending callback
	// future fires the suspend signal only when the caller actually
	// awaits the result, allowing intervening operations (like a
	// submitter step) to run first.
	preResultOnce sync.Once
	preResult     func()

	// deferredSuspension marks a future whose first Result call commits
	// the invocation to PENDING as a side effect (a pending callback).
	// Such a future cannot settle with a terminal outcome in the current
	// invocation, so [Any] and [Race] award terminal winners before
	// awaiting it: a losing pending callback must not force the
	// invocation to PENDING while a winner is returned.
	deferredSuspension bool
}

// Result blocks until the operation settles, then returns its outcome.
// Once the future has settled, Result returns immediately.
func (f *Future[O]) Result() (O, error) {
	if f.preResult != nil {
		f.preResultOnce.Do(f.preResult)
	}
	<-f.done
	return f.value, f.err
}

// settled reports whether the future has settled, without blocking and
// without running the pre-result hook. It exists for internal tests that
// must observe a future's state without awaiting it.
func (f *Future[O]) settled() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// settle resolves the future with value and err. Only the first call takes
// effect; subsequent calls are no-ops. This guarantees settle-once
// semantics regardless of race between normal completion and suspension.
func (f *Future[O]) settle(value O, err error) {
	f.once.Do(func() {
		f.value = value
		f.err = err
		close(f.done)
	})
}

// newFuture creates an unsettled future.
func newFuture[O any]() *Future[O] {
	return &Future[O]{done: make(chan struct{})}
}

// newPendingCallbackFuture creates a future for a callback that is still
// pending (STARTED or freshly checkpointed START). The suspend commitment is
// deferred to the first Result() call so that operations between
// CreateCallback and Result() (e.g. a submitter step in WaitForCallback) run
// in the same invocation.
func newPendingCallbackFuture[O any](s *suspendSignal, ec *execContext) *Future[O] {
	f := newFuture[O]()
	f.deferredSuspension = true
	registerFuture(s, f)
	// On the first Result(), mark this context blocked, commit the
	// invocation to PENDING, release the calling branch's token, and settle
	// the future with errSuspendExecution so the awaiting goroutine unwinds
	// and reports suspension to its coordinator. The branch-token accounting
	// keeps the commitment from globally suspending unrelated siblings.
	f.preResult = func() {
		ec.blocked.Store(true)
		s.commitPending(ec.abandon)
		ec.branchTok.release()
		var zero O
		f.settle(zero, errSuspendExecution)
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
	Err error
}

// settledJSON is the JSON-serializable form of Settled.
type settledJSON[O any] struct {
	Status string `json:"status"`
	Value  O      `json:"value,omitempty"`
	Err    string `json:"error,omitempty"`
}

// MarshalJSON serializes a Settled value. Errors are stored as their
// message string, so serialized outcomes carry no Go-specific error
// structure.
func (s Settled[O]) MarshalJSON() ([]byte, error) {
	j := settledJSON[O]{Status: "fulfilled", Value: s.Value}
	if s.Err != nil {
		j.Status = "rejected"
		j.Err = s.Err.Error()
		var zero O
		j.Value = zero
	}
	return json.Marshal(j)
}

// UnmarshalJSON deserializes a Settled value. Errors are reconstructed
// as [replayedError] values carrying the original message.
func (s *Settled[O]) UnmarshalJSON(data []byte) error {
	var j settledJSON[O]
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	if j.Status == "rejected" {
		s.Err = &replayedError{errType: "Error", message: j.Err}
		var zero O
		s.Value = zero
	} else {
		s.Value = j.Value
		s.Err = nil
	}
	return nil
}
