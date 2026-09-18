package durable

import (
	"sync"
	"sync/atomic"
)

// suspendSignal coordinates suspension of one invocation. It has two
// mechanisms:
//
//  1. Pending commitment: set when any operation encounters a state that
//     requires suspension (replayed STARTED wait, freshly checkpointed
//     START for a blocking operation, etc.). Once committed, the invocation
//     MUST return PENDING regardless of what user code does. This prevents
//     user code from swallowing errSuspendExecution and returning a bogus
//     success. A commitment made by an operation running under an
//     abandonable batch-branch subtree is recorded against that subtree's
//     abandon handle so it can be retired if the parent batch abandons the
//     branch after early completion; every other commitment is
//     unconditional.
//
//  2. Active-branch accounting: tracks the number of goroutines that can
//     independently make forward progress. When the last branch
//     deregisters, the signal fires: all registered futures are settled
//     with errSuspendExecution so goroutines blocked on Future.Result()
//     unwind without hanging. Branches that are still able to make
//     progress keep running and checkpointing until they too block.
//
// The fired() predicate is true once a commitment exists AND all active
// branches have deregistered (i.e., the signal has fired). User-facing code
// paths (the handler select, claimOperation) still use fired() to detect
// suspension.
type suspendSignal struct {
	once sync.Once
	ch   chan struct{}

	// mu guards firing, futures, active, rootCommitted, branchCommits, and
	// handleParent.
	mu      sync.Mutex
	futures []futureSettler

	// firing is set by fire while it holds mu, before fire drains futures.
	// From that moment no future is appended to futures: registerFuture
	// reads firing under mu and settles a new future itself. fire settles
	// the drained set and closes ch after releasing mu, so firing becomes
	// true before ch is closed. Checking ch alone would leave a window in
	// which a future registered after the drain is never settled.
	firing bool

	// active tracks the number of branches (goroutines) that can
	// independently make progress. When active reaches zero and a
	// commitment remains, the signal fires.
	active int

	// rootCommitted is set when an operation outside any abandonable
	// batch-branch subtree has committed the invocation to PENDING. It is
	// never retired.
	rootCommitted bool

	// branchCommits counts live pending commitments made under each
	// abandonable batch-branch subtree, keyed by the subtree's abandon
	// handle. A batch retires its handle's entry when it abandons the
	// branch after early completion, so abandoned work no longer forces
	// PENDING.
	branchCommits map[*atomic.Bool]int

	// handleParent records the enclosing abandon handle of each handle a
	// batch mints, or nil when the batch was minted outside any abandonable
	// subtree. It lets retirement cascade to handles minted beneath the
	// retiring handle, so a commitment made inside a nested batch is retired
	// when an enclosing batch abandons the branch that contains it. Entries
	// are removed when their handle is retired or when the batch that minted
	// it finishes without an outstanding commitment, so the map does not
	// grow across the many batches of a long-lived invocation.
	handleParent map[*atomic.Bool]*atomic.Bool
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
//
// fire sets firing and drains futures in one critical section. A future
// registered before that section is in the drained set and is settled
// below. A future registered after it observes firing and is settled by
// registerFuture. So every registered future is settled exactly once
// whatever the interleaving. ch is closed last, after the drained set has
// settled, so done and fired report suspension only once every future
// registered before fire has unwound.
func (s *suspendSignal) fire() {
	s.once.Do(func() {
		s.mu.Lock()
		s.firing = true
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
// returns true as soon as any live commitment exists, even if other
// branches are still active. The handler uses this to override a swallowed
// errSuspendExecution.
func (s *suspendSignal) committed() bool {
	s.mu.Lock()
	c := s.committedLocked()
	s.mu.Unlock()
	return c
}

// committedLocked reports whether a live commitment exists. Caller must
// hold mu.
func (s *suspendSignal) committedLocked() bool {
	if s.rootCommitted {
		return true
	}
	for _, n := range s.branchCommits {
		if n > 0 {
			return true
		}
	}
	return false
}

// commitPending marks the invocation as committed to PENDING. Called when
// an operation enters a blocking state (checkpointed START for wait/invoke,
// retry timer pending, etc.). retirable is the abandon handle of the
// operation's batch-branch subtree, or nil for an operation outside any
// abandonable subtree: a nil handle commits unconditionally, a non-nil one
// records a retirable commitment against that handle. If no active branches
// remain after this call, fires the signal immediately.
func (s *suspendSignal) commitPending(retirable *atomic.Bool) {
	s.mu.Lock()
	if retirable != nil {
		if s.branchCommits == nil {
			s.branchCommits = make(map[*atomic.Bool]int)
		}
		s.branchCommits[retirable]++
	} else {
		s.rootCommitted = true
	}
	shouldFire := s.active <= 0
	s.mu.Unlock()

	if shouldFire {
		s.fire()
	}
}

// registerHandle records the parentage of an abandon handle a batch has
// just minted. parent is the enclosing subtree's handle, or nil when the
// batch runs outside any abandonable subtree. Parentage lets retirement
// cascade to handles minted beneath a retiring handle, so nesting depth is
// unbounded and a commitment made inside a nested batch is retired when an
// enclosing batch abandons the branch that contains it.
func (s *suspendSignal) registerHandle(handle, parent *atomic.Bool) {
	if handle == nil {
		return
	}
	s.mu.Lock()
	if s.handleParent == nil {
		s.handleParent = make(map[*atomic.Bool]*atomic.Bool)
	}
	s.handleParent[handle] = parent
	s.mu.Unlock()
}

// forgetHandle drops the parentage entry for a handle whose batch finished
// with no outstanding commitment under it. It never touches branchCommits,
// so it cannot clear a commitment that still stands. It keeps handleParent
// bounded across the many batches of a long-lived invocation.
func (s *suspendSignal) forgetHandle(handle *atomic.Bool) {
	if handle == nil {
		return
	}
	s.mu.Lock()
	delete(s.handleParent, handle)
	s.mu.Unlock()
}

// descendsFromLocked reports whether handle h's ancestor chain reaches
// ancestor. Caller must hold mu.
func (s *suspendSignal) descendsFromLocked(h, ancestor *atomic.Bool) bool {
	for p := s.handleParent[h]; p != nil; p = s.handleParent[p] {
		if p == ancestor {
			return true
		}
	}
	return false
}

// retireCommitment removes the commitments recorded under an abandonable
// batch-branch subtree's abandon handle and every handle minted beneath it.
// A batch calls it after it has abandoned the branch and drained every
// worker, so a branch abandoned after early completion no longer forces the
// invocation to PENDING, including work done inside a nested batch that
// minted its own handle. It only removes commitments and never fires: it is
// invoked post-drain, and because nested batches are strictly nested in the
// call stack, draining the retiring batch's workers has already drained
// every nested worker, so no goroutine is left waiting on a future that
// firing would have settled.
func (s *suspendSignal) retireCommitment(retirable *atomic.Bool) {
	if retirable == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect the retiring handle plus every handle whose ancestor chain
	// reaches it. Reading the parentage map to completion before deleting
	// keeps each chain walk consistent.
	toRetire := map[*atomic.Bool]struct{}{retirable: {}}
	for h := range s.handleParent {
		if s.descendsFromLocked(h, retirable) {
			toRetire[h] = struct{}{}
		}
	}
	for h := range toRetire {
		delete(s.branchCommits, h)
		delete(s.handleParent, h)
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
	shouldFire := s.active <= 0 && s.committedLocked()
	s.mu.Unlock()

	if shouldFire {
		s.fire()
	}
}

// branchToken is an idempotent handle to one active-branch registration.
// The first release decrements the active count; later releases are no-ops,
// so a branch is accounted exactly once even when several unwind paths can
// each release it (a pending callback's pre-result hook and a deferred
// cleanup share one token).
type branchToken struct {
	s    *suspendSignal
	once sync.Once
}

// release deregisters the branch this token represents, at most once. Safe
// on a nil token and from any goroutine.
func (t *branchToken) release() {
	if t == nil {
		return
	}
	t.once.Do(t.s.deregisterBranch)
}

// registerBranchToken increments the active branch count and returns a token
// that releases it exactly once. Call before launching a goroutine that can
// independently make progress.
func (s *suspendSignal) registerBranchToken() *branchToken {
	s.registerBranch()
	return &branchToken{s: s}
}

// registerFuture adds a future to the in-flight set so that fire settles it
// with errSuspendExecution. It is called synchronously on the owning
// goroutine before the async operation's goroutine is launched.
//
// registerFuture needs no ordering guarantee from its caller. If fire has
// not yet started, the future is appended and fire's drain pass settles it.
// If fire has already started, fire set firing under mu and has already
// taken its copy of futures, so an append would never be seen; instead
// registerFuture settles the future immediately. firing is checked rather
// than ch because fire closes ch only after settling the drained set, so
// ch can still be open while the drain has already happened.
func registerFuture[O any](s *suspendSignal, f *Future[O]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.firing {
		var zero O
		f.settle(zero, errSuspendExecution)
		return
	}

	s.futures = append(s.futures, &futureSettlerAdapter[O]{f: f})
}
