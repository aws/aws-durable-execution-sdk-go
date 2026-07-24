package durable

import (
	"context"
	"sync"
	"time"
)

// dagSchedHooks decouples the scheduler's pure orchestration logic from the
// runtime, so the scheduler can be unit-tested with a fake runner (no
// checkpoints) while the real wiring plugs in the materialize/checkpoint and
// suspend coordination.
type dagSchedHooks struct {
	// runTask executes a single task body against its own child context and
	// returns the kind-erased result or an error.
	runTask func(def *dagTaskDef, deps Deps) (any, error)
	// register/deregister mirror active-goroutine accounting (nil-safe:
	// no-ops when unset). In alpha these are typically unset because
	// suspension is signaled directly through the invocation-wide suspend
	// channel rather than through active-count accounting.
	register   func()
	deregister func()
	// suspendedCh returns the channel that closes when the whole invocation
	// suspends (nil-safe: a nil channel never fires). In alpha this is
	// wired to the invocation-wide suspend signal.
	suspendedCh func() <-chan struct{}
	// isSuspend reports whether an error from runTask is the suspend
	// sentinel (nil-safe: treated as "never" when unset).
	isSuspend func(error) bool
	// now supplies timestamps (defaults to time.Now).
	now func() time.Time
}

// dagScheduler runs a validated task graph with bounded concurrency,
// trigger evaluation, runIf, skip propagation, and threshold/custom
// completion.
type dagScheduler struct {
	tasks      []*dagTaskDef
	maxConc    int
	completion *DagCompletionConfig
	hooks      dagSchedHooks
	// defaultTrigger is the DAG-level fallback trigger rule for tasks that
	// set none of their own. The empty value is treated as AllSuccess.
	defaultTrigger TriggerRule

	mu       sync.Mutex
	state    map[string]*TaskExecution // terminal (or STARTED) states by name
	inFlight map[string]struct{}

	// pending holds task completions delivered by worker goroutines but not
	// yet folded into state by the main loop. Completions are delivered
	// INTO this mutex-guarded queue (never via a separate channel) so that
	// the main loop's decision to park and a worker's decision to wake it
	// are serialized by the SAME lock — the single-lock protocol that
	// avoids the two-independent-signals spurious-wake race. Guarded by mu.
	pending []dagTaskDone
	// parentParked is true while the main loop goroutine is waiting for a
	// completion, so a finishing worker that observes it wakes the parent.
	// Guarded by mu.
	parentParked bool
	// wakeCh is created fresh under mu each time the main loop parks; a
	// finishing worker closes it to wake the parked parent. Guarded by mu.
	wakeCh chan struct{}
	// suspending is set once any task returns the suspend sentinel: the
	// whole invocation is going to suspend, so no new tasks start.
	suspending bool

	completing     bool
	completeReason DagCompletionReason
	completeSet    bool
}

type dagTaskDone struct {
	name   string
	result any
	err    error
	susp   bool
}

func newDagScheduler(tasks []*dagTaskDef, maxConc int, completion *DagCompletionConfig, hooks dagSchedHooks) *dagScheduler {
	if hooks.now == nil {
		hooks.now = time.Now
	}
	return &dagScheduler{
		tasks:      tasks,
		maxConc:    maxConc,
		completion: completion,
		hooks:      hooks,
		state:      map[string]*TaskExecution{},
		inFlight:   map[string]struct{}{},
	}
}

// run executes the graph and returns the per-task executions (registration
// order), the completion reason, and whether the invocation suspended.
func (s *dagScheduler) run(ctx context.Context) ([]TaskExecution, DagCompletionReason, bool) {
	for {
		// 1. Resolve skips synchronously and start ready tasks (bounded).
		progressed := s.startReady(ctx)

		s.mu.Lock()
		// 2. Drain any already-delivered completion FIRST, under the same
		// lock that guards the park decision below.
		if len(s.pending) > 0 {
			d := s.pending[0]
			s.pending = s.pending[1:]
			s.mu.Unlock()
			s.handleDone(d)
			continue
		}

		nInFlight := len(s.inFlight)
		allSettled := len(s.state) == len(s.tasks)
		completing := s.completing
		suspending := s.suspending

		if suspending && nInFlight == 0 {
			s.mu.Unlock()
			return nil, "", true
		}
		if (completing || allSettled) && nInFlight == 0 {
			s.mu.Unlock()
			break
		}
		if nInFlight == 0 && !progressed {
			// Drained: nothing in-flight and nothing new could start.
			s.mu.Unlock()
			break
		}
		if nInFlight == 0 {
			// A synchronous skip resolved this pass can free a dependent
			// that appears earlier than its dependency in registration
			// order (legal via After). Nothing in-flight would wake a
			// parked parent, so loop to re-scan rather than parking.
			s.mu.Unlock()
			continue
		}

		// 3. Park: mark ourselves parked and publish a fresh wake channel
		// ATOMICALLY with the (empty) pending check above.
		s.parentParked = true
		s.wakeCh = make(chan struct{})
		wakeCh := s.wakeCh
		s.mu.Unlock()

		if s.hooks.deregister != nil {
			s.hooks.deregister()
		}
		var suspCh <-chan struct{}
		if s.hooks.suspendedCh != nil {
			suspCh = s.hooks.suspendedCh()
		}
		select {
		case <-wakeCh:
			// A worker delivered a completion into s.pending. Loop to drain.
			if s.hooks.register != nil {
				s.hooks.register()
			}
		case <-suspCh:
			// Whole invocation suspended; in-flight goroutines are
			// abandoned exactly as the batch scheduler abandons its
			// branches.
			return nil, "", true
		}
	}

	return s.finalize()
}

// startReady evaluates readiness and starts (or skips) as many tasks as
// concurrency allows. Returns whether it changed any state.
func (s *dagScheduler) startReady(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.completing || s.suspending {
		return false
	}
	progressed := false

	for _, t := range s.tasks {
		if s.completing || s.suspending {
			break
		}
		if _, settled := s.state[t.name]; settled {
			continue
		}
		if _, running := s.inFlight[t.name]; running {
			continue
		}
		if !s.depsTerminalLocked(t) {
			continue
		}
		// Ready. Evaluate trigger rule from upstream statuses.
		upstream := s.upstreamStatusesLocked(t)
		rule := t.trigger
		if !t.hasTrigger || rule == "" {
			rule = s.defaultTrigger
		}
		if !evaluateTrigger(rule, upstream) {
			s.recordSkipLocked(t, SkipTriggerRule)
			progressed = true
			continue
		}
		deps := s.buildDepsLocked(t)
		if t.runIf != nil && !t.runIf(deps) {
			s.recordSkipLocked(t, SkipRunIf)
			progressed = true
			continue
		}
		// Respect concurrency bound (0 or negative = unbounded).
		if s.maxConc > 0 && len(s.inFlight) >= s.maxConc {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			continue
		}
		// Start the task in its own goroutine.
		s.inFlight[t.name] = struct{}{}
		progressed = true
		if s.hooks.register != nil {
			s.hooks.register()
		}
		go func(def *dagTaskDef, dp Deps) {
			result, err := s.hooks.runTask(def, dp)
			susp := err != nil && s.hooks.isSuspend != nil && s.hooks.isSuspend(err)
			if susp {
				s.deliverSuspend(def.name)
				return
			}
			s.deliverDone(dagTaskDone{name: def.name, result: result, err: err})
		}(t, deps)
	}
	return progressed
}

// deliverDone records a normal task completion into the mutex-guarded
// pending queue and, if the main loop has parked, wakes it. The completion
// and the read of parentParked happen under s.mu (single-lock protocol).
func (s *dagScheduler) deliverDone(d dagTaskDone) {
	s.wake(d)
}

// deliverSuspend records that a task suspended and wakes a parked parent.
func (s *dagScheduler) deliverSuspend(name string) {
	s.wake(dagTaskDone{name: name, susp: true})
}

// wake appends d to pending and wakes a parked main loop if present.
func (s *dagScheduler) wake(d dagTaskDone) {
	s.mu.Lock()
	s.pending = append(s.pending, d)
	var wakeCh chan struct{}
	if s.parentParked {
		s.parentParked = false
		wakeCh = s.wakeCh
	}
	s.mu.Unlock()
	if wakeCh != nil {
		close(wakeCh)
	}
}

// handleDone records a settled task and re-evaluates the completion policy.
func (s *dagScheduler) handleDone(d dagTaskDone) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.inFlight, d.name)
	if d.susp {
		// Task suspended; leave it unsettled so replay re-runs it, and stop
		// scheduling new tasks — the whole invocation is suspending.
		s.suspending = true
		return
	}
	if _, alreadyStarted := s.state[d.name]; alreadyStarted {
		// Early completion already recorded this as STARTED; drop result.
		return
	}
	now := s.hooks.now()
	te := &TaskExecution{Name: d.name, CompletedAt: now, kind: dagKindFor(s.tasks, d.name)}
	if d.err != nil {
		te.Status = StatusFailed
		te.Err = d.err
	} else {
		te.Status = StatusSucceeded
		te.result = d.result
	}
	s.state[d.name] = te
	s.evaluateCompletionLocked()
}

// evaluateCompletionLocked applies the threshold/custom completion policy.
func (s *dagScheduler) evaluateCompletionLocked() {
	if s.completing || s.completion == nil {
		return
	}
	if s.completion.isCustom() {
		status := s.completionStatusLocked()
		dec := s.completion.ShouldComplete(status)
		if dec.ShouldComplete() {
			if dec.Outcome() == OutcomeFailed {
				s.beginCompletingLocked(CustomCompletionFailed)
			} else {
				s.beginCompletingLocked(CustomCompletionSucceeded)
			}
		}
		return
	}
	if s.completion.isThreshold() {
		succ, fail, total := s.countsLocked()
		c := s.completion
		if c.ToleratedFailureCount != nil && fail > *c.ToleratedFailureCount {
			s.beginCompletingLocked(FailureToleranceExceeded)
			return
		}
		if c.ToleratedFailurePercentage != nil && total > 0 {
			if float64(fail)/float64(total)*100.0 > *c.ToleratedFailurePercentage {
				s.beginCompletingLocked(FailureToleranceExceeded)
				return
			}
		}
		if c.MinSuccessful != nil && succ >= *c.MinSuccessful {
			s.beginCompletingLocked(MinSuccessfulReached)
			return
		}
	}
}

// beginCompletingLocked marks the DAG as early-completing: no new tasks
// start, and every currently in-flight task is recorded as STARTED.
func (s *dagScheduler) beginCompletingLocked(reason DagCompletionReason) {
	s.completing = true
	s.completeReason = reason
	s.completeSet = true
	now := s.hooks.now()
	for name := range s.inFlight {
		if _, ok := s.state[name]; !ok {
			s.state[name] = &TaskExecution{Name: name, Status: StatusStarted, StartedAt: now, kind: dagKindFor(s.tasks, name)}
		}
	}
}

// finalize assembles the ordered executions and computes the completion
// reason for the drain (default) case.
func (s *dagScheduler) finalize() ([]TaskExecution, DagCompletionReason, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	execs := make([]TaskExecution, 0, len(s.tasks))
	failures := 0
	for _, t := range s.tasks {
		te, ok := s.state[t.name]
		if !ok {
			continue // never started: absent from results
		}
		if te.Status == StatusFailed {
			failures++
		}
		execs = append(execs, *te)
	}
	reason := s.completeReason
	if !s.completeSet {
		if failures > 0 {
			reason = CompletedWithFailures
		} else {
			reason = AllCompleted
		}
	}
	return execs, reason, false
}

// ── locked helpers ─────────────────────────────────────────────────────

func (s *dagScheduler) depsTerminalLocked(t *dagTaskDef) bool {
	for _, dep := range t.allDeps() {
		te, ok := s.state[dep]
		if !ok || te.Status == StatusStarted {
			return false
		}
	}
	return true
}

func (s *dagScheduler) upstreamStatusesLocked(t *dagTaskDef) []TaskStatus {
	deps := t.allDeps()
	out := make([]TaskStatus, 0, len(deps))
	for _, dep := range deps {
		if te, ok := s.state[dep]; ok {
			out = append(out, te.Status)
		}
	}
	return out
}

func (s *dagScheduler) buildDepsLocked(t *dagTaskDef) Deps {
	m := map[string]any{}
	for _, dep := range t.inlineDeps {
		if te, ok := s.state[dep]; ok && te.Status == StatusSucceeded {
			m[dep] = te.result
		}
	}
	return newDeps(m)
}

func (s *dagScheduler) recordSkipLocked(t *dagTaskDef, reason SkipReason) {
	now := s.hooks.now()
	s.state[t.name] = &TaskExecution{
		Name: t.name, Status: StatusSkipped, SkipReason: reason,
		StartedAt: now, CompletedAt: now, kind: dagKindFor(s.tasks, t.name),
	}
	// A skip is a settle: re-evaluate the completion policy so a custom
	// ShouldComplete predicate sees SKIPPED items and a skip-terminated DAG
	// reports the right reason.
	s.evaluateCompletionLocked()
}

func (s *dagScheduler) countsLocked() (succ, fail, total int) {
	for _, te := range s.state {
		switch te.Status {
		case StatusSucceeded:
			succ++
		case StatusFailed:
			fail++
		}
	}
	// total is the fixed number of tasks in the DAG, NOT the settled-so-far
	// count.
	return succ, fail, len(s.tasks)
}

func (s *dagScheduler) completionStatusLocked() DagCompletionStatus {
	st := DagCompletionStatus{Results: map[string]DagCompletionItemStatus{}}
	for _, t := range s.tasks {
		te, ok := s.state[t.name]
		if !ok {
			continue
		}
		item := DagCompletionItemStatus{Name: te.Name, Status: te.Status, SkipReason: te.SkipReason, result: te.result}
		st.Items = append(st.Items, item)
		st.Results[te.Name] = item
		st.CompletedCount++
		switch te.Status {
		case StatusSucceeded:
			st.SuccessCount++
		case StatusFailed:
			st.FailureCount++
		case StatusSkipped:
			st.SkippedCount++
		}
	}
	st.TotalCount = len(s.tasks)
	return st
}

func dagKindFor(tasks []*dagTaskDef, name string) dagResultKind {
	for _, t := range tasks {
		if t.name == name {
			return t.kind
		}
	}
	return dagKindPlain
}
