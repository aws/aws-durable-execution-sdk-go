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

	mu       sync.Mutex
	state    map[string]*TaskExecution // terminal (or STARTED) states by name
	inFlight map[string]struct{}

	completing bool
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
	done := make(chan taskDone)
	suspended := false

	for {
		// 1. Resolve skips synchronously and start ready tasks (bounded).
		progressed := s.startReady(ctx, done)

		s.mu.Lock()
		nInFlight := len(s.inFlight)
		allSettled := len(s.state) == len(s.tasks)
		completing := s.completing
		s.mu.Unlock()

		if (completing || allSettled) && nInFlight == 0 {
			break
		}
		if nInFlight == 0 && !progressed {
			// No in-flight work and nothing new could start: graph is
			// drained (remaining tasks, if any, are unreachable/skipped
			// already recorded). Done.
			break
		}

		// 2. Wait for a completion or a suspension.
		var suspCh <-chan struct{}
		if s.hooks.suspendedCh != nil {
			suspCh = s.hooks.suspendedCh()
		}
		select {
		case d := <-done:
			s.handleDone(d)
		case <-suspCh:
			// Whole invocation suspended; stop scheduling. In-flight
			// goroutines are abandoned exactly as the base batch
			// scheduler abandons its branches.
			suspended = true
			return nil, "", true
		}
	}

	if suspended {
		return nil, "", true
	}
	return s.finalize()
}

// startReady evaluates readiness and starts (or skips) as many tasks as
// concurrency allows. Returns whether it changed any state (started or
// skipped at least one task) — used to detect a drained graph.
func (s *scheduler) startReady(ctx context.Context, done chan taskDone) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.completing {
		return false
	}
	progressed := false

	for _, t := range s.tasks {
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
			rule = AllSuccess
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
			if !susp && s.hooks.deregister != nil {
				s.hooks.deregister()
			}
			done <- taskDone{name: def.name, result: result, err: err, susp: susp}
		}(t, deps)
	}
	return progressed
}

// handleDone records a settled task and re-evaluates the completion policy.
func (s *scheduler) handleDone(d taskDone) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.inFlight, d.name)
	if d.susp {
		// Task suspended; leave it unsettled so replay re-runs it.
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
	return succ, fail, len(s.state)
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
