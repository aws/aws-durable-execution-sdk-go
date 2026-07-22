package durable

import "sync"

// suspendSignal coordinates suspension of one invocation. Any durable
// operation that must wait for the backend (a scheduled retry, a wait, a
// pending callback) fires the signal; the handler observes it and ends the
// invocation with a PENDING response.
//
// Once fired, the signal is final for the invocation: the outcome of the
// user handler goroutine no longer changes the response. This mirrors the
// reference SDKs, where the suspension decision is recorded before user
// code unwinds, so user code that intercepts the suspension error cannot
// convert a suspended execution into a completed one.
//
// When fired, the signal also settles all registered in-flight futures with
// errSuspendExecution so that goroutines blocked on Future.Result() unwind
// without hanging.
type suspendSignal struct {
	once sync.Once
	ch   chan struct{}

	// mu guards the futures slice. Futures are registered before a
	// goroutine is launched (so registration is synchronous and
	// deterministic). On fire, all unsettled futures are settled with
	// errSuspendExecution.
	mu      sync.Mutex
	futures []futureSettler
}

// futureSettler is the settle interface for a type-erased future. Because
// Future is generic, the registry stores this interface to avoid type
// parameters on the signal.
type futureSettler interface {
	// settleWithSuspend settles the future with errSuspendExecution if it
	// has not already been settled.
	settleWithSuspend()
}

// futureSettlerAdapter wraps a *Future[O] to implement futureSettler.
type futureSettlerAdapter[O any] struct {
	f *Future[O]
}

func (a *futureSettlerAdapter[O]) settleWithSuspend() {
	var zero O
	a.f.settle(zero, errSuspendExecution)
}

func newSuspendSignal() *suspendSignal {
	return &suspendSignal{ch: make(chan struct{})}
}

// fire marks the invocation as suspending. Safe to call multiple times and
// from multiple goroutines. On the first call, all registered in-flight
// futures are settled with errSuspendExecution so blocked goroutines unwind.
func (s *suspendSignal) fire() {
	s.once.Do(func() {
		s.mu.Lock()
		fs := s.futures
		s.futures = nil // release references
		s.mu.Unlock()

		// Settle all in-flight futures so goroutines blocked on
		// Result() receive errSuspendExecution and unwind.
		for _, f := range fs {
			f.settleWithSuspend()
		}
		close(s.ch)
	})
}

// done returns a channel closed once the signal has fired.
func (s *suspendSignal) done() <-chan struct{} {
	return s.ch
}

// fired reports whether the signal has fired.
func (s *suspendSignal) fired() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// registerFuture adds a future to the in-flight set. If the signal has
// already fired, the future is settled immediately. This is called
// synchronously on the owning goroutine before launching the async
// operation's goroutine.
func registerFuture[O any](s *suspendSignal, f *Future[O]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If already fired, settle immediately (the close(s.ch) happened
	// after settling all existing futures; this one arrived late).
	select {
	case <-s.ch:
		var zero O
		f.settle(zero, errSuspendExecution)
		return
	default:
	}

	s.futures = append(s.futures, &futureSettlerAdapter[O]{f: f})
}
