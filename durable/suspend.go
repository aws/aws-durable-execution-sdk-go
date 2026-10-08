package durable

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// suspendSignal coordinates suspension of one invocation. The invocation
// ends with PENDING only when no goroutine running handler code can make
// progress in it. Four mechanisms decide that:
//
//  1. Active-branch accounting: counts the goroutines that run handler
//     code. Each holds a branch token from registration until it returns.
//
//  2. Parking: a goroutine that blocks inside the SDK, on an operation
//     that is not finished, on a [Future], or on the branches of a batch,
//     is counted as parked for as long as it blocks. The goroutine that
//     ends the block (a checkpoint response that reports the operation
//     finished, the settlement of the future) unparks it before it can
//     observe the change, so the parked count never includes a goroutine
//     that is about to resume.
//
//  3. Operation watches: each operation a goroutine is parked on has a
//     watch. Every checkpoint response is matched against the watches. A
//     response that reports a watched operation finished, or a step retry
//     or condition check due again (READY), resumes the goroutines parked
//     on it in this invocation. While an operation is watched and not
//     finished, the SDK polls its status with a checkpoint request that
//     carries no updates; see [pollSchedule].
//
//  4. Executing-span accounting: counts step attempts and condition checks
//     whose user code is running or whose outcome is being checkpointed,
//     and child contexts whose completion is being checkpointed.
//
// The invocation suspends when every active branch is parked or has
// returned, no checkpoint request is queued or in flight, no span is
// executing, and at least one watched operation is still not finished (or
// a commitment stands, see below). The conditions are checked again after
// a settle period, together with a generation stamp that every change to
// them advances, and the signal fires only if nothing changed. Firing
// settles every registered future and every parked operation waiter with
// errSuspendExecution, so blocked goroutines unwind.
//
// A commitment is a request to end the invocation with PENDING that no
// checkpoint response can withdraw. Only a [Race] over no futures makes
// one. A commitment made under an abandonable batch-branch subtree is
// recorded against that subtree's abandon handle, so it can be retired if
// the parent batch abandons the branch after early completion.
//
// Once fired, the invocation MUST return PENDING regardless of what user
// code does. This prevents user code from swallowing errSuspendExecution
// and returning a bogus success. The fired() predicate is true from the
// moment fire starts, before fire releases any goroutine. User-facing code
// paths (the handler outcome, claimOperation) use it to detect suspension.
type suspendSignal struct {
	once sync.Once
	ch   chan struct{}

	// mu guards firing, futures, active, rootCommitted, and branchCommits.
	mu      sync.Mutex
	futures []futureSettler

	// firing is set by fire while it holds mu, before fire drains futures.
	// From that moment no future is appended to futures: registerFuture
	// reads firing under mu and settles a new future itself. fire settles
	// the drained set and closes ch after releasing mu, so firing becomes
	// true before ch is closed. Checking ch alone would leave a window in
	// which a future registered after the drain is never settled.
	firing bool

	// active tracks the number of branches (goroutines) that run handler
	// code and have not returned. A branch that is parked still counts;
	// see parked.
	active int

	// activeDone is non-nil while active is above zero and is closed when
	// active returns to zero. awaitDrain waits on it so the response
	// follows the last deregistration without waiting out its settle
	// period.
	activeDone chan struct{}

	// executing counts spans whose outcome belongs to this invocation and
	// is not yet recorded: a step attempt or condition check from its
	// START checkpoint through the checkpoint of its outcome, and a child
	// context from the return of its body through the checkpoint of its
	// completion. The handler does not respond PENDING while executing is
	// above zero (see awaitDrain). Branches blocked on a pending
	// operation or running code between operations are not counted: they
	// have nothing to record that a later invocation cannot redo.
	executing int

	// executingGen advances on every enterExecuting. awaitDrain compares
	// it across its settle period to detect a span that began and ended
	// while the count was observed at zero.
	executingGen uint64

	// executingDone is non-nil while executing is above zero and is closed
	// when executing returns to zero. awaitDrain waits on it.
	executingDone chan struct{}

	// rootCommitted is set when an operation outside any abandonable
	// batch-branch subtree has committed the invocation to PENDING. It is
	// never retired.
	rootCommitted bool

	// branchCommits counts live pending commitments made under each
	// abandonable batch-branch subtree, keyed by the subtree's abandon
	// handle. A batch retires the entries of its handle and of every handle
	// beneath it when it abandons the branch after early completion, so
	// abandoned work no longer forces PENDING. Once a handle or any of its
	// ancestors is set, commitPending records nothing against it, so an
	// entry cannot reappear after retirement. Entries exist only while a
	// commitment stands, so the map stays bounded.
	branchCommits map[*abandonHandle]int

	// parked counts the active branches currently blocked inside the SDK.
	// active minus parked is the number of branches that can make
	// progress without an outside event.
	parked int

	// gen advances on every change to the suspension conditions: a branch
	// registers, deregisters, parks or unparks, a span begins or ends, a
	// checkpoint request is queued or settled, or an operation is watched.
	// maybeSuspend compares it across its settle period.
	gen uint64

	// settling is set while a settle period is being observed, so at most
	// one is observed at a time.
	settling bool

	// settle is the settle period. The zero value selects
	// defaultSuspendSettle.
	settle time.Duration

	// watches holds one watch per operation a goroutine is parked on,
	// keyed by wire operation ID. A watch exists only while it has
	// waiters.
	watches map[string]*opWatch

	// firedReason is the reason the watches stated when fire started,
	// before fire removed them; see suspendReason.
	firedReason string

	// checkpointBusy reports whether a checkpoint request is queued or in
	// flight. nil when no checkpointer is wired, as in unit tests.
	checkpointBusy func() bool

	// poller sends a checkpoint request that carries no updates. Its
	// response is matched against the watches like any other. nil
	// disables polling.
	poller func() error

	// deadline is the invocation deadline, zero when the invocation has
	// none. No poll is scheduled to fire within minPollRemaining of it.
	deadline time.Time

	// ctxDone is closed when the invocation's context ends. From then on
	// a running branch, an executing span, or a checkpoint in flight no
	// longer holds the response: the invocation suspends as soon as an
	// operation is awaited. nil when not wired.
	ctxDone <-chan struct{}

	// clock schedules polls. Tests replace it.
	clock pollClock

	// pollsStopped is set when polling ends for the invocation: on fire
	// and when the invocation's response is decided.
	pollsStopped bool
}

// abandonHandle marks one abandonable batch-branch subtree. A batch mints
// one handle when it takes the concurrent path and shares it with every
// context in its item subtrees. The batch sets the handle when it stops
// awaiting its outstanding branches after early completion.
//
// parent is the handle of the enclosing subtree, or nil when the batch runs
// outside any abandonable subtree. The chain is fixed at creation and is
// reachable from every context that holds the handle, so a handle's lineage
// lasts exactly as long as some branch under it can still act. A branch
// launched by durable.Go inside an item outlives the item worker and the
// batch that minted the handle; it still reaches every ancestor through the
// chain, so an enclosing batch's abandonment is visible to it without any
// registry the batch would have to keep current.
type abandonHandle struct {
	set    atomic.Bool
	parent *abandonHandle
}

// newAbandonHandle mints a handle beneath parent (nil at the top level).
// A handle minted beneath an already-abandoned parent is abandoned from
// the start, because abandoned reads the whole chain.
func newAbandonHandle(parent *abandonHandle) *abandonHandle {
	return &abandonHandle{parent: parent}
}

// abandon marks this subtree abandoned. Every handle beneath it observes
// the mark through abandoned.
func (h *abandonHandle) abandon() {
	h.set.Store(true)
}

// abandoned reports whether this subtree or any enclosing subtree has been
// abandoned. Safe to call on a nil handle, which is never abandoned.
func (h *abandonHandle) abandoned() bool {
	for ; h != nil; h = h.parent {
		if h.set.Load() {
			return true
		}
	}
	return false
}

// within reports whether h is ancestor or lies beneath it.
func (h *abandonHandle) within(ancestor *abandonHandle) bool {
	for ; h != nil; h = h.parent {
		if h == ancestor {
			return true
		}
	}
	return false
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
// settled, so done reports suspension only once every future registered
// before fire has unwound. fired reports it from the moment fire sets
// firing; see fired.
func (s *suspendSignal) fire() {
	s.once.Do(func() {
		s.mu.Lock()
		s.firing = true
		s.firedReason = s.watchReasonLocked()
		fs := s.futures
		s.futures = nil // release references
		// Every goroutine parked on an operation resumes with the
		// suspension sentinel, and no further poll is sent.
		s.pollsStopped = true
		for id, w := range s.watches {
			w.stopTimerLocked()
			for _, pw := range w.waiters {
				s.wakeLocked(pw, errSuspendExecution)
			}
			delete(s.watches, id)
		}
		s.mu.Unlock()

		// Settle all in-flight futures so goroutines blocked on
		// Result() receive errSuspendExecution and unwind.
		for _, f := range fs {
			f.settleWithSuspend()
		}
		close(s.ch)
	})
}

// done returns a channel that fire closes after it has settled every future
// registered before it. fired reports suspension earlier, from the moment
// fire starts.
func (s *suspendSignal) done() <-chan struct{} {
	return s.ch
}

// fired reports whether fire has started. It reads firing under mu instead
// of testing ch. fire sets firing before it wakes any parked goroutine or
// settles any future, and it closes ch only after it has settled them. A
// goroutine that fire releases can return from user code before ch is
// closed. If fired tested ch, a caller that runs after that return could
// see false, and the handler would respond SUCCEEDED for an invocation that
// is suspending. Reading firing reports true to every goroutine that fire
// has released.
func (s *suspendSignal) fired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firing
}

// committed reports whether the invocation is committed to PENDING: a
// commitment stands, or a goroutine is parked on an operation that is not
// finished. It returns true even if other branches are still active. The
// handler uses it when the handler returns while an orphaned branch is
// still blocked, and the park path of an unfinished replay uses it.
func (s *suspendSignal) committed() bool {
	s.mu.Lock()
	c := s.committedLocked()
	s.mu.Unlock()
	return c
}

// committedLocked reports whether a live commitment exists. Caller must
// hold mu.
func (s *suspendSignal) committedLocked() bool {
	if s.commitmentLocked() {
		return true
	}
	for _, w := range s.watches {
		for _, pw := range w.waiters {
			if !pw.detached {
				return true
			}
		}
	}
	return false
}

// commitmentLocked reports whether a commitment made by commitPending
// stands. Caller must hold mu.
func (s *suspendSignal) commitmentLocked() bool {
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
// an operation can never settle in this invocation (a [Race] over no
// futures). retirable is the abandon handle of the operation's
// batch-branch subtree, or nil for an operation outside any abandonable
// subtree: a nil handle commits unconditionally, a non-nil one records a
// retirable commitment against that handle. The signal fires once the
// suspension conditions hold; see maybeSuspend.
//
// A commitment against a handle whose subtree, or any enclosing subtree,
// has been abandoned is a no-op: nothing is recorded and the signal is not
// fired. A batch joins only its own item workers before it retires the
// commitments under its handle. A branch launched by durable.Go inside an
// item is not joined, so it can reach a blocking operation after
// retirement. Dropping its commitment here gives the same outcome as
// retiring it, whichever side of the retirement it lands on, so the
// invocation result does not depend on scheduling. The caller still
// returns errSuspendExecution and unwinds, as an abandoned branch does.
func (s *suspendSignal) commitPending(retirable *abandonHandle) {
	s.mu.Lock()
	if retirable != nil {
		if retirable.abandoned() {
			s.mu.Unlock()
			return
		}
		if s.branchCommits == nil {
			s.branchCommits = make(map[*abandonHandle]int)
		}
		s.branchCommits[retirable]++
	} else {
		s.rootCommitted = true
	}
	s.gen++
	s.mu.Unlock()

	s.maybeSuspend()
}

// retireCommitment marks an abandonable batch-branch subtree abandoned and
// removes the commitments recorded under its handle and under every handle
// minted beneath it. A batch calls it after it has abandoned the branch and
// drained its item workers, so a branch abandoned after early completion no
// longer forces the invocation to PENDING, including work done inside a
// nested batch that minted its own handle.
//
// Draining the item workers does not drain every goroutine under the
// subtree. A nested Map or Parallel runs inside an item worker, so its
// workers are joined before the item worker returns. A branch launched by
// durable.Go inside an item is not joined by anything: it holds its own
// branch token and settles its own future when its operation returns. Such
// a branch can commit before retirement (removed here) or after it (dropped
// by commitPending, because the handle it carries reaches the retiring
// handle through its parent chain); either way no commitment stands, so
// the outcome is the same. The chain also covers a durable.Go launched
// inside a nested batch's item after that nested batch has returned, and
// a nested batch that the durable.Go branch starts after retirement: both
// carry a handle beneath the retiring one.
//
// retireCommitment only removes commitments and never fires. Goroutines
// parked on an operation under the subtree are resumed by wakeAbandoned,
// which the batch calls when it abandons the branch.
func (s *suspendSignal) retireCommitment(retirable *abandonHandle) {
	if retirable == nil {
		return
	}
	retirable.abandon()
	s.mu.Lock()
	defer s.mu.Unlock()
	for h := range s.branchCommits {
		if h.within(retirable) {
			delete(s.branchCommits, h)
		}
	}
}

// registerBranch increments the active branch count. Call before launching
// a goroutine that can independently make progress.
func (s *suspendSignal) registerBranch() {
	s.mu.Lock()
	if s.active <= 0 {
		s.activeDone = make(chan struct{})
	}
	s.active++
	s.gen++
	s.mu.Unlock()
}

// deregisterBranch decrements the active branch count and checks the
// suspension conditions. Safe to call after the signal has already fired.
func (s *suspendSignal) deregisterBranch() {
	s.mu.Lock()
	s.active--
	s.gen++
	if s.active <= 0 && s.activeDone != nil {
		close(s.activeDone)
		s.activeDone = nil
	}
	s.mu.Unlock()

	s.maybeSuspend()
}

// enterExecuting records that an executing span has started: a step
// attempt, a condition check, or a child context's completion record. Pair
// with exitExecuting. Every call advances executingGen, so a drain that
// observed the count at zero can tell that a span began afterwards.
func (s *suspendSignal) enterExecuting() {
	s.mu.Lock()
	if s.executing == 0 {
		s.executingDone = make(chan struct{})
	}
	s.executing++
	s.executingGen++
	s.gen++
	s.mu.Unlock()
}

// exitExecuting records that a span recorded by enterExecuting has recorded
// its outcome. When no span remains, waiters on executingDone are released.
func (s *suspendSignal) exitExecuting() {
	s.mu.Lock()
	s.executing--
	if s.executing <= 0 {
		s.executing = 0
		if s.executingDone != nil {
			close(s.executingDone)
			s.executingDone = nil
		}
	}
	s.gen++
	s.mu.Unlock()

	s.maybeSuspend()
}

// awaitDrain blocks until no branch of this invocation can record further
// work, or until ctx ends. The handler calls it after its goroutine has
// unwound with errSuspendExecution and before it responds PENDING, so an
// executing span on another branch finishes and records its outcome in
// this invocation instead of being refused and repeated by the next one.
//
// awaitDrain returns as soon as one of these holds:
//
//  1. No branch other than the handler goroutine is registered. Every
//     executing span runs on a registered branch, so none can begin.
//  2. No span is executing, and none began during settle. A span ends
//     when its outcome is checkpointed and the branch that ran it may
//     begin the next one at once (a step returning into its child
//     context, which then records its own completion), so a single
//     observation of a zero count is not stable: the count is read
//     again after settle, together with the generation stamp that every
//     enterExecuting advances, and the wait resumes if either changed.
//  3. ctx is done. The invocation's deadline bounds the wait, so a long
//     span cannot hold the response past it; its later checkpoint is
//     refused by the terminated checkpointer.
//
// Branches that are blocked on a future, or running code between
// operations, are not executing and are not waited for beyond settle:
// they hold nothing a later invocation cannot redo. A span that begins
// after awaitDrain returns is refused at its next checkpoint, as for any
// orphaned branch.
func (s *suspendSignal) awaitDrain(ctx context.Context, settle time.Duration) {
	for {
		s.mu.Lock()
		if s.active <= 0 {
			s.mu.Unlock()
			return
		}
		if s.executing > 0 {
			done := s.executingDone
			s.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return
			}
		}
		gen := s.executingGen
		idle := s.activeDone
		s.mu.Unlock()

		timer := time.NewTimer(settle)
		select {
		case <-timer.C:
		case <-idle:
			// The last branch deregistered: condition 1 holds, which
			// the recheck below confirms without waiting out settle.
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			return
		}

		s.mu.Lock()
		stable := s.active <= 0 || (s.executing == 0 && s.executingGen == gen)
		s.mu.Unlock()
		if stable {
			return
		}
	}
}

// branchToken is an idempotent handle to one active-branch registration.
// The first release decrements the active count; later releases are no-ops,
// so a branch is accounted exactly once even when several unwind paths can
// each release it (the park path of an unfinished replay and a deferred
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

	// The future parks the goroutines that await it on this signal, and
	// its settlement unparks them under mu.
	f.s = s
	if s.firing {
		var zero O
		f.settleLocked(zero, errSuspendExecution)
		return
	}

	s.futures = append(s.futures, &futureSettlerAdapter[O]{f: f})
}
