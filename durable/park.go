package durable

import (
	"errors"
	"time"
)

// Poll schedule constants. While a goroutine is parked on an operation that
// is not finished, the SDK polls the operation's status with a checkpoint
// request that carries no updates.
const (
	// initialPollDelay is the delay before the first poll of an operation
	// that has no known end time: a callback or a chained invoke.
	initialPollDelay = time.Second

	// pollIntervalCeiling caps the interval between polls that find no
	// change, up to pollsBeforeExtendedInterval polls.
	pollIntervalCeiling = 10 * time.Second

	// extendedPollInterval is the interval between polls once more than
	// pollsBeforeExtendedInterval polls have found no change.
	extendedPollInterval = 60 * time.Second

	// pollsBeforeExtendedInterval is the number of unchanged polls after
	// which the interval widens to extendedPollInterval.
	pollsBeforeExtendedInterval = 95

	// minPollRemaining is the least time before the invocation deadline at
	// which a poll is still sent. A poll costs a checkpoint round trip, and
	// the handler needs time to act on its result.
	minPollRemaining = time.Second
)

// pollSchedule returns the delay before the next poll of an operation after
// polls polls have found no change: min(polls, 10) seconds while polls is at
// most pollsBeforeExtendedInterval, and extendedPollInterval after that.
func pollSchedule(polls int) time.Duration {
	if polls > pollsBeforeExtendedInterval {
		return extendedPollInterval
	}
	d := time.Duration(polls) * time.Second
	if d > pollIntervalCeiling {
		d = pollIntervalCeiling
	}
	return d
}

// pollTimer is a scheduled poll that can be cancelled.
type pollTimer interface {
	Stop() bool
}

// pollClock is the time source of the poll schedule. The default is the
// system clock; tests substitute a manual one.
type pollClock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) pollTimer
}

// systemClock is the pollClock backed by package time.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) AfterFunc(d time.Duration, f func()) pollTimer { return time.AfterFunc(d, f) }

// parkWaiter is one goroutine parked on an operation or on one or more
// futures. Its fields are guarded by suspendSignal.mu.
type parkWaiter struct {
	// ch is closed when the waiter is woken.
	ch chan struct{}

	// woken is set when ch is closed.
	woken bool

	// parked reports whether the waiter is counted in suspendSignal.parked.
	parked bool

	// err is errSuspendExecution when the waiter was woken because the
	// invocation suspends or because its batch branch was abandoned; nil
	// when the awaited operation reached the awaited status.
	err error

	// abandon is the abandon handle of the waiter's batch-branch subtree,
	// nil outside any abandonable subtree.
	abandon *abandonHandle

	// detached marks a waiter that does not hold the invocation at
	// PENDING when the handler returns while it is parked: a pending
	// callback awaited through a combinator that may already have
	// returned another future's outcome.
	detached bool
}

// awaitGate records whether anything awaits the future of an asynchronous
// operation started by StepAsync, WaitAsync, or InvokeAsync. Until the
// future is awaited, the goroutine that runs the operation parks as a
// detached waiter. So a future that nothing awaits does not hold the
// invocation at PENDING when the handler returns. When the future is
// awaited, the waiters already parked become attached, and later parks
// are attached. Its fields are guarded by suspendSignal.mu.
type awaitGate struct {
	// awaited is set at the first await of the future.
	awaited bool

	// waiters are the detached waiters parked before the first await.
	waiters []*parkWaiter
}

// markAwaited records the first await of the future gated by g, and
// attaches every waiter parked on its operation so far. Later calls have
// no effect.
func (s *suspendSignal) markAwaited(g *awaitGate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g.awaited {
		return
	}
	g.awaited = true
	for _, w := range g.waiters {
		w.detached = false
	}
	g.waiters = nil
	s.gen++
}

// newWaiterLocked returns a parked waiter. Caller holds mu.
func (s *suspendSignal) newWaiterLocked(abandon *abandonHandle, detached bool) *parkWaiter {
	w := &parkWaiter{ch: make(chan struct{}), abandon: abandon, detached: detached}
	w.parked = true
	s.parked++
	s.gen++
	return w
}

// wakeLocked wakes w with err and stops counting it as parked. Waking a
// waiter twice has no further effect. Caller holds mu.
func (s *suspendSignal) wakeLocked(w *parkWaiter, err error) {
	if w.woken {
		return
	}
	w.woken = true
	w.err = err
	if w.parked {
		w.parked = false
		s.parked--
		s.gen++
	}
	close(w.ch)
}

// parker counts one goroutine as parked while it waits on a channel that
// other branches signal, such as a batch coordinator waiting for its item
// workers. A signaling branch calls signal before it sends, and the waiting
// goroutine calls received after each receive. parked and pending change
// under suspendSignal.mu, so the waiting goroutine is never counted as
// parked while a signal is pending: park checks pending in the same
// critical section that counts the goroutine as parked, and signal clears
// the parked count in the same critical section that adds to pending.
type parker struct {
	s       *suspendSignal
	parked  bool // guarded by s.mu
	pending int  // signals not yet received; guarded by s.mu
}

// newParker returns an unparked parker on s.
func (s *suspendSignal) newParker() *parker {
	return &parker{s: s}
}

// park counts the goroutine as parked, unless a signal is pending, and
// checks the suspension conditions.
func (p *parker) park() {
	p.s.mu.Lock()
	if !p.parked && p.pending == 0 {
		p.parked = true
		p.s.parked++
		p.s.gen++
	}
	p.s.mu.Unlock()
	p.s.maybeSuspend()
}

// signal records one pending signal and stops counting the goroutine as
// parked. The signaling branch calls it before it sends.
func (p *parker) signal() {
	p.s.mu.Lock()
	p.pending++
	if p.parked {
		p.parked = false
		p.s.parked--
	}
	p.s.gen++
	p.s.mu.Unlock()
}

// received consumes one pending signal. The waiting goroutine calls it
// after each receive.
func (p *parker) received() {
	p.s.mu.Lock()
	p.pending--
	if p.parked {
		p.parked = false
		p.s.parked--
	}
	p.s.gen++
	p.s.mu.Unlock()
}

// opWatch tracks one operation that at least one goroutine is parked on.
// Its fields are guarded by suspendSignal.mu.
type opWatch struct {
	wireID string

	// ready reports whether the operation's record lets the waiters
	// resume: a terminal status, or READY for a step retry or condition
	// check.
	ready func(*operation) bool

	// endTime returns the time at which the operation is expected to
	// change, or zero when it has none. The first poll is sent then.
	endTime func(*operation) time.Time

	// record returns the operation's current record, or nil when it has
	// none. It names the kind of event the invocation is pending on when
	// the invocation suspends.
	record func() *operation

	waiters []*parkWaiter

	// status is the operation's status the watch last observed.
	status operationStatus

	// timer is the scheduled poll, nil when none is scheduled.
	timer pollTimer

	// polls counts the polls that found no change.
	polls int

	// epoch advances whenever the scheduled poll is cancelled, so a poll
	// whose timer fired before the cancellation can tell it is stale.
	epoch uint64
}

// stopTimerLocked cancels the scheduled poll. Caller holds mu.
func (w *opWatch) stopTimerLocked() {
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	w.epoch++
}

// awaitOperation blocks the calling goroutine until the record of the
// operation with positional ID id satisfies ready, and returns that record.
// It returns at once when the current record already does.
//
// While it blocks, the goroutine is parked and the operation is watched:
// every checkpoint response is matched against the watch, and the SDK
// polls the operation's status on the poll schedule, starting at
// endTime(record) or, when that is zero, initialPollDelay after the
// goroutine blocks. It returns errSuspendExecution when the invocation
// suspends, or when the batch-branch subtree of abandon is abandoned,
// before the operation is ready.
//
// detached marks a goroutine whose block does not hold the invocation at
// PENDING when the handler returns; see parkWaiter.detached. gate, when
// non-nil, makes the block detached until the gated future is awaited;
// see awaitGate.
func (s *suspendSignal) awaitOperation(state *executionState, id string, abandon *abandonHandle, detached bool, gate *awaitGate, ready func(*operation) bool, endTime func(*operation) time.Time) (*operation, error) {
	wireID := hashID(id)
	s.mu.Lock()
	if s.firing || abandon.abandoned() {
		s.mu.Unlock()
		return nil, errSuspendExecution
	}
	// The record is read under mu. A checkpoint response merges into the
	// state before it is matched against the watches under mu, so either
	// this read sees the merged record or the match sees this watch.
	op := state.get(id)
	if ready(op) {
		s.mu.Unlock()
		return op, nil
	}
	gated := gate != nil && !gate.awaited
	w := s.newWaiterLocked(abandon, detached || gated)
	if gated {
		gate.waiters = append(gate.waiters, w)
	}
	watch := s.watches[wireID]
	if watch == nil {
		if s.watches == nil {
			s.watches = make(map[string]*opWatch)
		}
		watch = &opWatch{
			wireID:  wireID,
			ready:   ready,
			endTime: endTime,
			record:  func() *operation { return state.get(id) },
		}
		if op != nil {
			watch.status = op.status
		}
		s.watches[wireID] = watch
		s.armFirstPollLocked(watch, op)
	}
	watch.waiters = append(watch.waiters, w)
	s.mu.Unlock()

	s.maybeSuspend()
	<-w.ch
	if w.err != nil {
		return nil, w.err
	}
	return state.get(id), nil
}

// armFirstPollLocked schedules the first poll of watch: at the operation's
// end time, or initialPollDelay from now when it has none. Caller holds mu.
func (s *suspendSignal) armFirstPollLocked(watch *opWatch, op *operation) {
	now := s.clockLocked().Now()
	delay := initialPollDelay
	if end := watch.endTime(op); !end.IsZero() {
		delay = max(end.Sub(now), 0)
	}
	s.armPollLocked(watch, delay)
}

// armPollLocked schedules a poll of watch after delay, unless polling is
// off or the poll would fire less than minPollRemaining before the
// invocation deadline. Caller holds mu.
func (s *suspendSignal) armPollLocked(watch *opWatch, delay time.Duration) {
	watch.stopTimerLocked()
	if s.pollsStopped || s.poller == nil {
		return
	}
	clock := s.clockLocked()
	if !s.deadline.IsZero() && clock.Now().Add(delay).After(s.deadline.Add(-minPollRemaining)) {
		return
	}
	epoch := watch.epoch
	watch.timer = clock.AfterFunc(delay, func() { s.pollWatch(watch, epoch) })
}

// clockLocked returns the poll clock. Caller holds mu.
func (s *suspendSignal) clockLocked() pollClock {
	if s.clock == nil {
		s.clock = systemClock{}
	}
	return s.clock
}

// pollWatch sends one poll for watch, unless the watch has ended or its
// poll was cancelled since this one was scheduled. When the response
// reports no change to the operation's status, the next poll is scheduled
// by pollSchedule.
func (s *suspendSignal) pollWatch(watch *opWatch, epoch uint64) {
	s.mu.Lock()
	if s.pollsStopped || s.watches[watch.wireID] != watch || watch.epoch != epoch {
		s.mu.Unlock()
		return
	}
	watch.timer = nil
	if !s.deadline.IsZero() && s.deadline.Sub(s.clockLocked().Now()) < minPollRemaining {
		s.mu.Unlock()
		return
	}
	poll := s.poller
	s.mu.Unlock()

	err := poll()

	s.mu.Lock()
	// A response that reported a change cancelled this poll's schedule
	// and, when the operation is still not ready, scheduled a new first
	// poll. Only an unchanged watch continues this schedule.
	if !errors.Is(err, errCheckpointTerminated) && s.watches[watch.wireID] == watch && watch.epoch == epoch {
		watch.polls++
		s.armPollLocked(watch, pollSchedule(watch.polls))
	}
	s.mu.Unlock()
	s.maybeSuspend()
}

// onStateMerged matches the records of one checkpoint response against the
// watches. A record that satisfies its watch wakes the watch's waiters. A
// record whose status differs from the status the watch last observed, but
// that does not satisfy it, cancels the scheduled poll and schedules a new
// first poll.
func (s *suspendSignal) onStateMerged(ops []*operation) {
	s.mu.Lock()
	for _, op := range ops {
		watch := s.watches[op.id]
		if watch == nil {
			continue
		}
		if watch.ready(op) {
			watch.stopTimerLocked()
			for _, w := range watch.waiters {
				s.wakeLocked(w, nil)
			}
			delete(s.watches, op.id)
			continue
		}
		if op.status != watch.status {
			watch.status = op.status
			s.armFirstPollLocked(watch, op)
			s.gen++
		}
	}
	s.mu.Unlock()
}

// wakeAbandoned resumes, with errSuspendExecution, every goroutine parked
// on an operation under an abandoned batch-branch subtree. A batch calls
// it after it abandons its outstanding branches, so their workers unwind
// and the batch can drain them.
func (s *suspendSignal) wakeAbandoned() {
	s.mu.Lock()
	for id, watch := range s.watches {
		kept := watch.waiters[:0]
		for _, w := range watch.waiters {
			if w.abandon.abandoned() {
				s.wakeLocked(w, errSuspendExecution)
				continue
			}
			kept = append(kept, w)
		}
		watch.waiters = kept
		if len(kept) == 0 {
			watch.stopTimerLocked()
			delete(s.watches, id)
		}
	}
	s.mu.Unlock()
	s.maybeSuspend()
}

// stopPolls ends polling for the invocation. The handler calls it once the
// invocation's response is decided.
func (s *suspendSignal) stopPolls() {
	s.mu.Lock()
	s.pollsStopped = true
	for _, watch := range s.watches {
		watch.stopTimerLocked()
	}
	s.mu.Unlock()
}

// touch records a change to the suspension conditions made outside
// suspendSignal, such as a checkpoint request being queued.
func (s *suspendSignal) touch() {
	s.mu.Lock()
	s.gen++
	s.mu.Unlock()
}

// shouldSuspendLocked reports whether the suspension conditions hold:
// every active branch is parked, no checkpoint request is queued or in
// flight, no span is executing, and an operation is still awaited or a
// commitment stands. Once the invocation's context has ended, only the
// last condition applies. Caller holds mu.
func (s *suspendSignal) shouldSuspendLocked() bool {
	if s.firing {
		return false
	}
	if s.contextEnded() {
		// The invocation's context bounds every wait: work still under
		// way cannot hold the response past it. Its later checkpoints
		// are refused by the terminated checkpointer.
		return len(s.watches) > 0 || s.commitmentLocked()
	}
	if s.active-s.parked > 0 || s.executing > 0 {
		return false
	}
	if s.checkpointBusy != nil && s.checkpointBusy() {
		return false
	}
	return len(s.watches) > 0 || s.commitmentLocked()
}

// contextEnded reports whether the invocation's context has ended.
func (s *suspendSignal) contextEnded() bool {
	if s.ctxDone == nil {
		return false
	}
	select {
	case <-s.ctxDone:
		return true
	default:
		return false
	}
}

// maybeSuspend checks the suspension conditions. When they hold, it
// observes a settle period and fires the signal if they still hold and
// nothing changed in between. A change during the period is not lost: the
// conditions are checked again at its end.
func (s *suspendSignal) maybeSuspend() {
	s.mu.Lock()
	if s.settling || !s.shouldSuspendLocked() {
		s.mu.Unlock()
		return
	}
	s.settling = true
	gen := s.gen
	settle := s.settle
	if settle <= 0 {
		settle = defaultSuspendSettle
	}
	s.mu.Unlock()

	go func() {
		time.Sleep(settle)
		s.mu.Lock()
		s.settling = false
		stable := s.shouldSuspendLocked() && s.gen == gen
		s.mu.Unlock()
		if stable {
			s.fire()
			return
		}
		s.maybeSuspend()
	}()
}

// terminalRecord reports whether op is a terminal record: the readiness
// condition of a wait, a callback, and a chained invoke.
func terminalRecord(op *operation) bool {
	return op != nil && op.status.terminal()
}

// attemptDueRecord reports whether op lets a step retry or a condition
// check continue: its next attempt is due (READY), or it is terminal.
func attemptDueRecord(op *operation) bool {
	return op != nil && (op.status == statusReady || op.status.terminal())
}

// noEndTime is the end time of an operation that has none: a callback or a
// chained invoke. Its first poll is sent initialPollDelay after the
// goroutine blocks.
func noEndTime(*operation) time.Time { return time.Time{} }

// nextAttemptTime returns the end time of a step retry or condition check:
// the next attempt's scheduled time from op, else fallback.
func nextAttemptTime(fallback time.Time) func(*operation) time.Time {
	return func(op *operation) time.Time {
		if op != nil && op.step != nil && !op.step.nextAttempt.IsZero() {
			return op.step.nextAttempt
		}
		return fallback
	}
}

// awaitOperation parks the calling goroutine until the record of the
// operation id satisfies ready; see suspendSignal.awaitOperation. When the
// invocation suspends or the context's batch branch is abandoned first, it
// marks the context blocked, so user code that swallows the error cannot
// start further operations on it, and returns errSuspendExecution.
func (c *execContext) awaitOperation(id string, ready func(*operation) bool, endTime func(*operation) time.Time) (*operation, error) {
	op, err := c.suspend.awaitOperation(c.state, id, c.abandon, false, c.gate, ready, endTime)
	if err != nil {
		c.blocked.Store(true)
		return nil, err
	}
	return op, nil
}
