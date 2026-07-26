package durable

import "sync"

// suspendSignal coordinates suspension of one invocation. It has two
// mechanisms:
//
//  1. pendingCommitted: set (atomically, once) when any operation encounters
//     a state that requires suspension (replayed STARTED wait, freshly
//     checkpointed START for a blocking operation, etc.). Once committed,
//     the invocation MUST return PENDING regardless of what user code does.
//     This prevents user code from swallowing errSuspendExecution and
//     returning a bogus success.
//
//  2. Active-branch accounting: tracks the number of goroutines that can
//     independently make forward progress. When the last branch
//     deregisters, the signal fires: all registered futures are settled
//     with errSuspendExecution so goroutines blocked on Future.Result()
//     unwind without hanging. Branches that are still able to make
//     progress keep running and checkpointing until they too block.
//
// The fired() predicate is true once pendingCommitted has been set AND all
// active branches have deregistered (i.e., the signal has fired). User-
// facing code paths (the handler select, claimOperation) still use fired()
// to detect suspension.
type suspendSignal struct {
	once sync.Once
	ch   chan struct{}

	// mu guards futures, active, and pendingCommitted.
	mu      sync.Mutex
	futures []futureSettler

	// active tracks the number of branches (goroutines) that can
	// independently make progress. When active reaches zero and
	// pendingCommitted is true, the signal fires.
	active int

	// pendingCommitted is set when any operation has entered a state
	// that commits the invocation to PENDING. Once set, the invocation
	// result is always PENDING.
	pendingCommitted bool
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

// fired reports whether the signal has fired (all branches exhausted).
func (s *suspendSignal) fired() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// committed reports whether the invocation is committed to PENDING. This
// returns true as soon as any operation enters a blocking state, even if
// other branches are still active. The handler uses this to override a
// swallowed errSuspendExecution.
func (s *suspendSignal) committed() bool {
	s.mu.Lock()
	c := s.pendingCommitted
	s.mu.Unlock()
	return c
}

// commitPending marks the invocation as committed to PENDING. Called when
// an operation enters a blocking state (checkpointed START for wait/invoke,
// retry timer pending, etc.). If no active branches remain after this call,
// fires the signal immediately.
func (s *suspendSignal) commitPending() {
	s.mu.Lock()
	s.pendingCommitted = true
	shouldFire := s.active <= 0
	s.mu.Unlock()

	if shouldFire {
		s.fire()
	}
}

// registerBranch increments the active branch count. Call before launching
// a goroutine that can independently make progress.
func (s *suspendSignal) registerBranch() {
	s.mu.Lock()
	s.active++
	s.mu.Unlock()
}

// deregisterBranch decrements the active branch count. If the count
// reaches zero and the invocation is committed to PENDING, fires the
// suspension signal. Safe to call after the signal has already fired.
func (s *suspendSignal) deregisterBranch() {
	s.mu.Lock()
	s.active--
	shouldFire := s.active <= 0 && s.pendingCommitted
	s.mu.Unlock()

	if shouldFire {
		s.fire()
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
