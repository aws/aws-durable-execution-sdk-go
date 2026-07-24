package dag

import (
	"context"
	"sync"
	"time"
)

// schedHooks decouples the scheduler's pure orchestration logic from the
// base-SDK runtime, so the scheduler can be unit-tested with a fake runner
// (no checkpoints) while the real wiring (Task 9) plugs in execmgr's
// register/deregister/suspend coordination.
type schedHooks struct {
	// runTask executes a single task body against its own name-derived
	// child context and returns the kind-erased result or an error.
	runTask func(def *taskDef, deps Deps) (any, error)
	// register/deregister mirror execmgr's active-goroutine accounting
	// (nil-safe: no-ops when unset, e.g. in tests).
	register   func()
	deregister func()
	// suspendedCh returns the channel that closes when the whole
	// invocation suspends (nil-safe: a nil channel never fires).
	suspendedCh func() <-chan struct{}
	// isSuspend reports whether an error from runTask is the suspend
	// sentinel (nil-safe: treated as "never" when unset).
	isSuspend func(error) bool
	// now supplies timestamps (defaults to time.Now).
	now func() time.Time
}

// scheduler runs a validated task graph with bounded concurrency, trigger
// evaluation, runIf, skip propagation, and threshold/custom completion.
type scheduler struct {
	tasks      []*taskDef
	maxConc    int
	completion *DagCompletionConfig
	hooks      schedHooks
	// defaultTrigger is the DAG-level fallback trigger rule for tasks that
	// set none of their own (WithDefaultTriggerRule). The empty value is
	// treated as AllSuccess by evaluateTrigger.
	defaultTrigger TriggerRule

	mu       sync.Mutex
	state    map[string]*TaskExecution // terminal (or STARTED) states by name
	inFlight map[string]struct{}

	// pending holds task completions delivered by worker goroutines but
	// not yet folded into state by the main loop. Completions are
	// delivered INTO this mutex-guarded queue (never via a separate
	// channel) so that the main loop's decision to park (deregister) and a
	// worker's decision to hand a registration back are serialized by the
	// SAME lock - this is the single-lock protocol the base batchScheduler
	// uses to avoid the two-independent-signals spurious-suspension bug
	// (see batch.go runBatch bugs #1/#2). Guarded by mu.
	pending []taskDone
	// parentParked mirrors the base batch scheduler's parentDeregistered:
	// true while the main loop goroutine has deregistered itself and is
	// waiting for a completion, so the finishing worker that observes it
	// hands a registration back (before its own Deregister) and the active
	// count never spuriously hits zero. Cleared by whichever worker
	// performs the hand-off. Guarded by mu.
	parentParked bool
	// wakeCh is created fresh under mu each time the main loop parks; the
	// hand-off worker closes it to wake the parked parent. A per-park
	// channel (rather than a reused buffered one) means there is never a
	// stale wake token to cause a spurious unpark. Guarded by mu.
	wakeCh chan struct{}
	// suspending is set once any task returns the suspend sentinel: the
	// whole invocation is going to suspend, so no new tasks start.
	suspending bool

	completing     bool
	completeReason CompletionReason
	completeSet    bool
}

type taskDone struct {
	name   string
	result any
	err    error
	susp   bool
}

func newScheduler(tasks []*taskDef, maxConc int, completion *DagCompletionConfig, hooks schedHooks) *scheduler {
	if hooks.now == nil {
		hooks.now = time.Now
	}
	return &scheduler{
		tasks:      tasks,
		maxConc:    maxConc,
		completion: completion,
		hooks:      hooks,
		state:      map[string]*TaskExecution{},
		inFlight:   map[string]struct{}{},
	}
}

// run executes the graph and returns the per-task executions (registration
// order), the completion reason, and whether the invocation suspended
// (in which case the caller must propagate the suspend signal, and the
// returned executions/reason are not authoritative).
func (s *scheduler) run(ctx context.Context) ([]TaskExecution, CompletionReason, bool) {
	for {
		// 1. Resolve skips synchronously and start ready tasks (bounded).
		progressed := s.startReady(ctx)

		s.mu.Lock()
		// 2. Drain any already-delivered completion FIRST, under the same
		// lock that guards the park decision below. This is the crux of the
		// single-lock protocol: a worker delivers its completion into
		// s.pending under s.mu, so if any completion arrived we observe it
		// here and never proceed to park. Draining and the park decision
		// therefore cannot straddle an independent signal.
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
			// Reaching here implies progressed==true (the !progressed drain
			// above already returned). A synchronous skip resolved THIS pass
			// can free a dependent that appears EARLIER than its dependency
			// in registration order (legal via DependsOn ordering edges) and
			// so was skipped over by startReady's single forward pass. There
			// is nothing in-flight to ever wake a parked parent, so parking
			// here would deadlock the pure-logic path and spuriously suspend
			// the real-runtime path (active count -> 0 => Suspended fires).
			// Instead loop to re-scan; this terminates because every
			// progressing pass settles >=1 task and any task actually
			// started leaves nInFlight>0 (so we do reach the park below).
			s.mu.Unlock()
			continue
		}

		// 3. Park: mark ourselves parked and publish a fresh wake channel
		// ATOMICALLY with the (empty) pending check above (same lock hold).
		// A worker finishing concurrently now either (a) took s.mu before
		// us, in which case it appended to s.pending and we would have
		// drained it above rather than reaching here, or (b) takes s.mu
		// after us, sees parentParked, and performs the Register hand-off
		// before its own Deregister - so the active count never spuriously
		// reaches zero while a completion is pending.
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
			// A worker delivered a completion into s.pending. For a normal
			// completion it also performed the Register hand-off on our
			// behalf (before its own Deregister), so we are registered
			// again without an explicit Register here. Loop to drain.
		case <-suspCh:
			// Whole invocation suspended; in-flight goroutines are
			// abandoned exactly as the base batch scheduler abandons its
			// branches.
			return nil, "", true
		}
	}

	return s.finalize()
}

// startReady evaluates readiness and starts (or skips) as many tasks as
// concurrency allows. Returns whether it changed any state (started or
// skipped at least one task) — used to detect a drained graph.
func (s *scheduler) startReady(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.completing || s.suspending {
		return false
	}
	progressed := false

	for _, t := range s.tasks {
		// A skip (below) may trigger custom/threshold completion mid-loop;
		// once completing, start no further tasks.
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
			// Fall back to the DAG-level default (empty => AllSuccess,
			// handled by evaluateTrigger).
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
		go func(def *taskDef, dp Deps) {
			result, err := s.hooks.runTask(def, dp)
			susp := err != nil && s.hooks.isSuspend != nil && s.hooks.isSuspend(err)
			if susp {
				s.deliverSuspend(def.name)
				return
			}
			s.deliverDone(taskDone{name: def.name, result: result, err: err})
		}(t, deps)
	}
	return progressed
}

// deliverDone records a normal task completion into the mutex-guarded
// pending queue and, if the main loop has parked, performs the execmgr
// Register hand-off (BEFORE this goroutine's own Deregister) then wakes it.
// This is the single-lock protocol ported from the base batchScheduler
// (batch.go branchFinished): the completion, the read of parentParked, and
// the hand-off decision all happen under s.mu, so the active count can
// never spuriously reach zero while a completion is pending.
func (s *scheduler) deliverDone(d taskDone) {
	s.mu.Lock()
	s.pending = append(s.pending, d)
	handoff := s.parentParked
	var wakeCh chan struct{}
	if handoff {
		s.parentParked = false
		wakeCh = s.wakeCh
	}
	s.mu.Unlock()

	if handoff && s.hooks.register != nil {
		// Register on the parent's behalf BEFORE our own Deregister so the
		// two overlap for one instant instead of the count touching zero.
		s.hooks.register()
	}
	if s.hooks.deregister != nil {
		s.hooks.deregister()
	}
	if handoff {
		close(wakeCh)
	}
}

// deliverSuspend records that a task suspended and wakes a parked parent.
// Unlike deliverDone it performs NO Register/Deregister: the operation that
// suspended already deregistered this goroutine before blocking (base SDK
// contract), and letting the active count fall is what lets the genuine
// invocation-wide suspension fire (via the Suspended channel).
func (s *scheduler) deliverSuspend(name string) {
	s.mu.Lock()
	s.pending = append(s.pending, taskDone{name: name, susp: true})
	parked := s.parentParked
	var wakeCh chan struct{}
	if parked {
		s.parentParked = false
		wakeCh = s.wakeCh
	}
	s.mu.Unlock()
	if parked {
		close(wakeCh)
	}
}

// handleDone records a settled task and re-evaluates the completion policy.
func (s *scheduler) handleDone(d taskDone) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.inFlight, d.name)
	if d.susp {
		// Task suspended; leave it unsettled so replay re-runs it, and
		// stop scheduling new tasks - the whole invocation is suspending.
		s.suspending = true
		return
	}
	if _, alreadySTARTED := s.state[d.name]; alreadySTARTED {
		// Early completion already recorded this as STARTED; drop result.
		return
	}
	now := s.hooks.now()
	te := &TaskExecution{Name: d.name, CompletedAt: now, kind: kindFor(s.tasks, d.name)}
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
func (s *scheduler) evaluateCompletionLocked() {
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
func (s *scheduler) beginCompletingLocked(reason CompletionReason) {
	s.completing = true
	s.completeReason = reason
	s.completeSet = true
	now := s.hooks.now()
	for name := range s.inFlight {
		if _, ok := s.state[name]; !ok {
			s.state[name] = &TaskExecution{Name: name, Status: StatusStarted, StartedAt: now, kind: kindFor(s.tasks, name)}
		}
	}
}

// finalize assembles the ordered executions and computes the completion
// reason for the drain (default) case.
func (s *scheduler) finalize() ([]TaskExecution, CompletionReason, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	execs := make([]TaskExecution, 0, len(s.tasks))
	failures := 0
	for _, t := range s.tasks {
		te, ok := s.state[t.name]
		if !ok {
			continue // never started (unreachable): absent from results
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

func (s *scheduler) depsTerminalLocked(t *taskDef) bool {
	for _, dep := range t.allDeps() {
		te, ok := s.state[dep]
		if !ok || te.Status == StatusStarted {
			return false
		}
	}
	return true
}

func (s *scheduler) upstreamStatusesLocked(t *taskDef) []TaskStatus {
	deps := t.allDeps()
	out := make([]TaskStatus, 0, len(deps))
	for _, dep := range deps {
		if te, ok := s.state[dep]; ok {
			out = append(out, te.Status)
		}
	}
	return out
}

func (s *scheduler) buildDepsLocked(t *taskDef) Deps {
	m := map[string]any{}
	for _, dep := range t.inlineDeps {
		if te, ok := s.state[dep]; ok && te.Status == StatusSucceeded {
			m[dep] = te.result
		}
	}
	return newDeps(m)
}

func (s *scheduler) recordSkipLocked(t *taskDef, reason SkipReason) {
	now := s.hooks.now()
	s.state[t.name] = &TaskExecution{
		Name: t.name, Status: StatusSkipped, SkipReason: reason,
		StartedAt: now, CompletedAt: now, kind: kindFor(s.tasks, t.name),
	}
	// A skip is a settle: re-evaluate the completion policy so a custom
	// ShouldComplete predicate sees SKIPPED items and a skip-terminated DAG
	// reports the right reason (spec §2.10 - "each time a task settles").
	s.evaluateCompletionLocked()
}

func (s *scheduler) countsLocked() (succ, fail, total int) {
	for _, te := range s.state {
		switch te.Status {
		case StatusSucceeded:
			succ++
		case StatusFailed:
			fail++
		}
	}
	// total is the fixed number of tasks in the DAG (matching the base
	// batchCompletion.total = len(items)), NOT the settled-so-far count -
	// otherwise the ToleratedFailurePercentage denominator would be the
	// number of tasks settled so far, making the first failure read as
	// 100% and tripping any tolerance immediately.
	return succ, fail, len(s.tasks)
}

func (s *scheduler) completionStatusLocked() DagCompletionStatus {
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

func kindFor(tasks []*taskDef, name string) resultKind {
	for _, t := range tasks {
		if t.name == name {
			return t.kind
		}
	}
	return kindPlain
}
