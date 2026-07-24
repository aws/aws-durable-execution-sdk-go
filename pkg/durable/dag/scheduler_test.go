package dag

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runSched builds a scheduler over d's tasks with a fake runner and runs it.
func runSched(d *Context, maxConc int, completion *DagCompletionConfig, runTask func(def *taskDef, deps Deps) (any, error)) ([]TaskExecution, CompletionReason) {
	s := newScheduler(d.tasks, maxConc, completion, schedHooks{runTask: runTask})
	execs, reason, susp := s.run(context.Background())
	if susp {
		panic("unexpected suspend in test")
	}
	return execs, reason
}

func statusByName(execs []TaskExecution) map[string]TaskStatus {
	m := map[string]TaskStatus{}
	for _, e := range execs {
		m[e.Name] = e.Status
	}
	return m
}

func TestScheduler_TopologicalOrder(t *testing.T) {
	d := newContext("")
	fetch := Step(d, "fetch", nil, func(_ Deps, _ StepContext) (int, error) { return 1, nil })
	a := Step(d, "a", []AnyHandle{fetch}, func(_ Deps, _ StepContext) (int, error) { return 2, nil })
	b := Step(d, "b", []AnyHandle{fetch}, func(_ Deps, _ StepContext) (int, error) { return 3, nil })
	Step(d, "merge", []AnyHandle{a, b}, func(_ Deps, _ StepContext) (int, error) { return 4, nil })

	var mu sync.Mutex
	order := []string{}
	execs, reason := runSched(d, 0, nil, func(def *taskDef, _ Deps) (any, error) {
		mu.Lock()
		order = append(order, def.name)
		mu.Unlock()
		return 0, nil
	})

	if reason != AllCompleted {
		t.Fatalf("reason=%v", reason)
	}
	// fetch before a,b before merge.
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	if !(pos["fetch"] < pos["a"] && pos["fetch"] < pos["b"] && pos["a"] < pos["merge"] && pos["b"] < pos["merge"]) {
		t.Fatalf("bad topological order: %v", order)
	}
	if len(execs) != 4 {
		t.Fatalf("want 4 execs, got %d", len(execs))
	}
}

func TestScheduler_PeakConcurrencyBounded(t *testing.T) {
	d := newContext("")
	for i := 0; i < 20; i++ {
		Step(d, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	var cur, peak int64
	limit := 4
	runSched(d, limit, nil, func(def *taskDef, _ Deps) (any, error) {
		n := atomic.AddInt64(&cur, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if n <= p || atomic.CompareAndSwapInt64(&peak, p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt64(&cur, -1)
		return 0, nil
	})
	if peak > int64(limit) {
		t.Fatalf("peak in-flight %d exceeded limit %d", peak, limit)
	}
}

func TestScheduler_SkipCascade(t *testing.T) {
	d := newContext("")
	root := Step(d, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, errors.New("boom") })
	// child requires root success (default ALL_SUCCESS) -> skipped.
	child := Step(d, "child", []AnyHandle{root}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	// grandchild depends on child -> also skipped (cascade).
	Step(d, "grandchild", []AnyHandle{child}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

	execs, reason := runSched(d, 0, nil, func(def *taskDef, _ Deps) (any, error) {
		if def.name == "root" {
			return 0, errors.New("boom")
		}
		return 0, nil
	})
	st := statusByName(execs)
	if st["root"] != StatusFailed || st["child"] != StatusSkipped || st["grandchild"] != StatusSkipped {
		t.Fatalf("bad skip cascade: %v", st)
	}
	if reason != CompletedWithFailures {
		t.Fatalf("reason=%v want CompletedWithFailures", reason)
	}
}

func TestScheduler_RunIfSkip(t *testing.T) {
	d := newContext("")
	Step(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	execs, _ := runSched(d, 0, nil, func(def *taskDef, _ Deps) (any, error) { return 0, nil })
	st := statusByName(execs)
	if st["a"] != StatusSkipped {
		t.Fatalf("runIf=false should skip, got %v", st)
	}
	if execs[0].SkipReason != SkipRunIf {
		t.Fatalf("skip reason=%v", execs[0].SkipReason)
	}
}

func TestScheduler_TriggerAllFailedRunsCompensation(t *testing.T) {
	d := newContext("")
	charge := Step(d, "charge", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	Step(d, "refund", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }).
		DependsOn(charge).WithTrigger(AllFailed)
	ran := map[string]bool{}
	var mu sync.Mutex
	execs, _ := runSched(d, 0, nil, func(def *taskDef, _ Deps) (any, error) {
		mu.Lock()
		ran[def.name] = true
		mu.Unlock()
		if def.name == "charge" {
			return 0, errors.New("declined")
		}
		return 0, nil
	})
	st := statusByName(execs)
	if st["charge"] != StatusFailed {
		t.Fatalf("charge should fail")
	}
	if st["refund"] != StatusSucceeded {
		t.Fatalf("refund (ALL_FAILED) should run+succeed, got %v", st)
	}
}

func TestScheduler_ThresholdMinSuccessful(t *testing.T) {
	d := newContext("")
	for i := 0; i < 5; i++ {
		Step(d, fmt.Sprintf("r%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	min := 2
	_, reason := runSched(d, 1, &DagCompletionConfig{MinSuccessful: &min}, func(def *taskDef, _ Deps) (any, error) {
		return 0, nil
	})
	if reason != MinSuccessfulReached {
		t.Fatalf("reason=%v want MinSuccessfulReached", reason)
	}
}

func TestScheduler_CustomCompletion(t *testing.T) {
	d := newContext("")
	for i := 0; i < 5; i++ {
		Step(d, fmt.Sprintf("rule%d", i), nil, func(_ Deps, _ StepContext) (string, error) { return "OK", nil })
	}
	completion := &DagCompletionConfig{
		ShouldComplete: func(st DagCompletionStatus) CompletionDecision {
			for _, it := range st.Items {
				if it.Status == StatusSucceeded {
					if v, ok := ResultOf[string](it); ok && v == "REJECT" {
						return CompleteDag(OutcomeFailed)
					}
				}
			}
			return ContinueDag()
		},
	}
	// No REJECT -> drains fully -> AllCompleted.
	_, reason := runSched(d, 2, completion, func(def *taskDef, _ Deps) (any, error) { return "OK", nil })
	if reason != AllCompleted {
		t.Fatalf("reason=%v want AllCompleted (no reject)", reason)
	}

	// One REJECT -> custom failed.
	_, reason2 := runSched(d, 1, completion, func(def *taskDef, _ Deps) (any, error) {
		if def.name == "rule2" {
			return "REJECT", nil
		}
		return "OK", nil
	})
	if reason2 != CustomCompletionFailed {
		t.Fatalf("reason=%v want CustomCompletionFailed", reason2)
	}
}

func TestScheduler_DrainByDefaultDoesNotCancelSiblings(t *testing.T) {
	d := newContext("")
	Step(d, "fails", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	Step(d, "ok", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	execs, reason := runSched(d, 2, nil, func(def *taskDef, _ Deps) (any, error) {
		if def.name == "fails" {
			return 0, errors.New("x")
		}
		return 0, nil
	})
	st := statusByName(execs)
	if st["fails"] != StatusFailed || st["ok"] != StatusSucceeded {
		t.Fatalf("drain-by-default should run both: %v", st)
	}
	if reason != CompletedWithFailures {
		t.Fatalf("reason=%v", reason)
	}
}

func TestScheduler_DepValuesFlowThroughDeps(t *testing.T) {
	d := newContext("")
	src := Step(d, "src", nil, func(_ Deps, _ StepContext) (int, error) { return 21, nil })
	var got int
	Step(d, "dbl", []AnyHandle{src}, func(deps Deps, _ StepContext) (int, error) {
		v, err := Get(deps, src)
		if err != nil {
			return 0, err
		}
		got = v * 2
		return got, nil
	})
	// Fake runner delegates to the real def.run? No: use real fn via a
	// closure that calls def.run with a nil ctx is not possible here, so
	// instead assert Deps propagation through the scheduler by having the
	// runner read the injected deps for "dbl".
	execs, _ := runSched(d, 0, nil, func(def *taskDef, deps Deps) (any, error) {
		switch def.name {
		case "src":
			return 21, nil
		case "dbl":
			v, err := Get(deps, src)
			if err != nil {
				return 0, err
			}
			return v * 2, nil
		}
		return nil, nil
	})
	st := statusByName(execs)
	if st["dbl"] != StatusSucceeded {
		t.Fatalf("dbl status=%v", st)
	}
	for _, e := range execs {
		if e.Name == "dbl" {
			if e.result.(int) != 42 {
				t.Fatalf("dep value did not flow: got %v", e.result)
			}
		}
	}
}

// TestScheduler_DefaultTriggerRuleApplied verifies that a DAG-level default
// trigger rule (WithDefaultTriggerRule) is actually honored for tasks that
// set none of their own — a task depending on a FAILED upstream must run
// under a default of AllDone (rather than being skipped under the built-in
// AllSuccess). Guards against the default being silently ignored.
func TestScheduler_DefaultTriggerRuleApplied(t *testing.T) {
	d := newContext("")
	root := Step(d, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	// "comp" sets no per-task trigger; it should inherit the DAG default.
	Step(d, "comp", []AnyHandle{root}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

	s := newScheduler(d.tasks, 0, nil, schedHooks{runTask: func(def *taskDef, _ Deps) (any, error) {
		if def.name == "root" {
			return 0, errors.New("boom")
		}
		return 0, nil
	}})
	s.defaultTrigger = AllDone
	execs, reason, susp := s.run(context.Background())
	if susp {
		t.Fatal("unexpected suspend")
	}
	st := statusByName(execs)
	if st["root"] != StatusFailed {
		t.Fatalf("root should fail, got %v", st["root"])
	}
	if st["comp"] != StatusSucceeded {
		t.Fatalf("comp should RUN under default AllDone (not skip), got %v", st["comp"])
	}
	if reason != CompletedWithFailures {
		t.Fatalf("reason=%v want CompletedWithFailures", reason)
	}

	// Control: with the built-in default (AllSuccess, empty defaultTrigger),
	// the same graph skips "comp".
	d2 := newContext("")
	root2 := Step(d2, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	Step(d2, "comp", []AnyHandle{root2}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	execs2, _ := runSched(d2, 0, nil, func(def *taskDef, _ Deps) (any, error) {
		if def.name == "root" {
			return 0, errors.New("boom")
		}
		return 0, nil
	})
	if statusByName(execs2)["comp"] != StatusSkipped {
		t.Fatalf("control: comp should skip under AllSuccess default")
	}
}

// TestScheduler_EmptyDag verifies an empty graph drains cleanly to
// AllCompleted with no executions.
func TestScheduler_EmptyDag(t *testing.T) {
	d := newContext("")
	execs, reason := runSched(d, 0, nil, func(def *taskDef, _ Deps) (any, error) { return nil, nil })
	if len(execs) != 0 {
		t.Fatalf("empty dag should have 0 execs, got %d", len(execs))
	}
	if reason != AllCompleted {
		t.Fatalf("empty dag reason=%v want AllCompleted", reason)
	}
}

// raceExecMgr is a faithful in-test model of execmgr.Manager's
// active-goroutine accounting and Suspended-channel semantics (the SAME
// close-at-zero / swap-fresh-channel-on-register-from-zero behavior), so
// the scheduler's park/hand-off protocol is exercised against a real
// suspension signal rather than a no-op fake. See execmgr.go.
type raceExecMgr struct {
	mu     sync.Mutex
	active int
	susp   chan struct{}
}

func newRaceExecMgr() *raceExecMgr { return &raceExecMgr{susp: make(chan struct{})} }

func (m *raceExecMgr) Register() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active++
	if m.active == 1 {
		select {
		case <-m.susp:
			m.susp = make(chan struct{})
		default:
		}
	}
}

func (m *raceExecMgr) Deregister() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active--
	if m.active <= 0 {
		select {
		case <-m.susp:
		default:
			close(m.susp)
		}
	}
}

func (m *raceExecMgr) Suspended() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.susp
}

// TestScheduler_NoSpuriousSuspension is the regression guard for the
// park+hand-off BLOCKER: it runs a bounded-concurrency DAG whose tasks all
// complete normally (never suspend) many times against a real execmgr
// model, and asserts the scheduler NEVER reports suspension. Before the
// single-lock fix, the parent's lock-free pre-drain of the done channel
// could race its decision to park, letting the final select pick
// suspension for a DAG whose tasks had all completed (reproduced 5/20000).
func TestScheduler_NoSpuriousSuspension(t *testing.T) {
	const iters = 8000
	const n = 6
	for it := 0; it < iters; it++ {
		d := newContext("")
		for i := 0; i < n; i++ {
			Step(d, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
		}
		em := newRaceExecMgr()
		em.Register() // parent (handler) goroutine enters run() registered
		hooks := schedHooks{
			runTask: func(_ *taskDef, _ Deps) (any, error) {
				// A tiny delay widens the window where a worker completes
				// right as the parent parks - the exact race being guarded.
				time.Sleep(3 * time.Microsecond)
				return 0, nil
			},
			register:    em.Register,
			deregister:  em.Deregister,
			suspendedCh: em.Suspended,
			isSuspend:   func(error) bool { return false },
		}
		s := newScheduler(d.tasks, 2, nil, hooks)
		execs, _, susp := s.run(context.Background())
		if susp {
			t.Fatalf("iteration %d: spurious suspension for a DAG whose tasks all completed", it)
		}
		if len(execs) != n {
			t.Fatalf("iteration %d: want %d execs, got %d", it, n, len(execs))
		}
	}
}

// TestScheduler_ThresholdFailurePercentage guards fix #2: the
// ToleratedFailurePercentage denominator is the fixed task count, not the
// settled-so-far count. With maxConc=1 (deterministic order) a single
// failure among 4 tasks is 25% and must NOT trip a 50% tolerance (before
// the fix the first failure read as 100% and tripped immediately).
func TestScheduler_ThresholdFailurePercentage(t *testing.T) {
	pct := 50.0

	// Only the first task fails => 25% of 4 => under tolerance => full drain.
	d := newContext("")
	for i := 0; i < 4; i++ {
		Step(d, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	execs, reason := runSched(d, 1, &DagCompletionConfig{ToleratedFailurePercentage: &pct}, func(def *taskDef, _ Deps) (any, error) {
		if def.name == "t0" {
			return 0, errors.New("boom")
		}
		return 0, nil
	})
	if reason != CompletedWithFailures {
		t.Fatalf("one failure of four (25%%) must not trip 50%% tolerance; reason=%v", reason)
	}
	if len(execs) != 4 {
		t.Fatalf("all four tasks should have drained, got %d", len(execs))
	}

	// Enough failures to genuinely exceed 50% => trips FailureToleranceExceeded.
	d2 := newContext("")
	for i := 0; i < 4; i++ {
		Step(d2, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	_, reason2 := runSched(d2, 1, &DagCompletionConfig{ToleratedFailurePercentage: &pct}, func(def *taskDef, _ Deps) (any, error) {
		return 0, errors.New("boom") // all fail; at t2 => 3/4 = 75% > 50%
	})
	if reason2 != FailureToleranceExceeded {
		t.Fatalf("majority failures should trip 50%% tolerance; reason=%v", reason2)
	}
}

// TestScheduler_CustomCompletionAfterSkip guards fix #3: a custom
// ShouldComplete predicate is evaluated after a SKIP settles (spec §2.10).
// A root failure skips its child; the predicate completes-with-failure the
// moment it observes a SKIPPED item. Before the fix, recordSkip did not
// re-evaluate completion, so the DAG drained to CompletedWithFailures and
// the custom reason was lost.
func TestScheduler_CustomCompletionAfterSkip(t *testing.T) {
	d := newContext("")
	root := Step(d, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	// child inherits default ALL_SUCCESS; root fails => child skipped.
	Step(d, "child", []AnyHandle{root}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

	completion := &DagCompletionConfig{
		ShouldComplete: func(st DagCompletionStatus) CompletionDecision {
			for _, it := range st.Items {
				if it.Status == StatusSkipped {
					return CompleteDag(OutcomeFailed)
				}
			}
			return ContinueDag()
		},
	}
	_, reason := runSched(d, 1, completion, func(def *taskDef, _ Deps) (any, error) {
		if def.name == "root" {
			return 0, errors.New("boom")
		}
		return 0, nil
	})
	if reason != CustomCompletionFailed {
		t.Fatalf("custom predicate must fire on a SKIP settle; reason=%v want CustomCompletionFailed", reason)
	}
}

// runSchedTimeout runs the scheduler but fails the test (rather than hanging
// the whole run) if it does not return within d. The park BLOCKER manifests
// on the pure-logic path as a permanent deadlock, so a timeout is the only
// way to surface it as a fast, clear failure.
func runSchedTimeout(t *testing.T, sc *scheduler, d time.Duration) ([]TaskExecution, CompletionReason, bool) {
	t.Helper()
	type res struct {
		execs  []TaskExecution
		reason CompletionReason
		susp   bool
	}
	ch := make(chan res, 1)
	go func() {
		e, r, s := sc.run(context.Background())
		ch <- res{e, r, s}
	}()
	select {
	case r := <-ch:
		return r.execs, r.reason, r.susp
	case <-time.After(d):
		t.Fatalf("scheduler.run did not return within %v (park deadlock)", d)
		return nil, "", false
	}
}

// TestScheduler_SkipFreesEarlierDependent is the regression guard for the
// park BLOCKER (loop-2): a synchronous skip resolved in one startReady pass
// can free a dependent that appears EARLIER in registration order (legal via
// a DependsOn ordering edge) and so is not re-scanned that pass. With zero
// tasks in-flight the parent must NOT park (no worker exists to wake it):
// the pure-logic path would deadlock and the real-runtime path would
// spuriously suspend. All prior skip/cascade tests register dependents AFTER
// dependencies, so they never exercised this ordering.
func TestScheduler_SkipFreesEarlierDependent(t *testing.T) {
	// ── single upstream ──────────────────────────────────────────────
	// y registered BEFORE x; y depends on x; x is skipped synchronously
	// (runIf=false). y then skips (cascade under default AllSuccess).
	d := newContext("")
	y := Step(d, "y", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	x := Step(d, "x", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	y.DependsOn(x)

	sc := newScheduler(d.tasks, 0, nil, schedHooks{
		runTask:   func(def *taskDef, _ Deps) (any, error) { return 0, nil },
		isSuspend: func(error) bool { return false },
	})
	execs, reason, susp := runSchedTimeout(t, sc, 2*time.Second)
	if susp {
		t.Fatal("single-upstream: spurious suspension for a fully-resolvable DAG")
	}
	st := statusByName(execs)
	if st["x"] != StatusSkipped || st["y"] != StatusSkipped {
		t.Fatalf("single-upstream: want both skipped, got %v", st)
	}
	if reason != AllCompleted {
		t.Fatalf("single-upstream: reason=%v want AllCompleted", reason)
	}

	// ── multi upstream ───────────────────────────────────────────────
	// y registered BEFORE x1,x2; both upstreams skipped synchronously in
	// the same pass; y is freed only after they settle and must not park.
	d2 := newContext("")
	y2 := Step(d2, "y", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	x1 := Step(d2, "x1", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	x2 := Step(d2, "x2", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	y2.DependsOn(x1, x2)

	sc2 := newScheduler(d2.tasks, 0, nil, schedHooks{
		runTask:   func(def *taskDef, _ Deps) (any, error) { return 0, nil },
		isSuspend: func(error) bool { return false },
	})
	execs2, reason2, susp2 := runSchedTimeout(t, sc2, 2*time.Second)
	if susp2 {
		t.Fatal("multi-upstream: spurious suspension for a fully-resolvable DAG")
	}
	st2 := statusByName(execs2)
	if st2["x1"] != StatusSkipped || st2["x2"] != StatusSkipped || st2["y"] != StatusSkipped {
		t.Fatalf("multi-upstream: want all skipped, got %v", st2)
	}
	if reason2 != AllCompleted {
		t.Fatalf("multi-upstream: reason=%v want AllCompleted", reason2)
	}

	// ── real-runtime model: same inverted-order synchronous skip must not
	// spuriously suspend when execmgr accounting/Suspended is wired in. ──
	d3 := newContext("")
	y3 := Step(d3, "y", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	x3 := Step(d3, "x", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	y3.DependsOn(x3)
	em := newRaceExecMgr()
	em.Register() // parent goroutine enters run() registered
	sc3 := newScheduler(d3.tasks, 0, nil, schedHooks{
		runTask:     func(def *taskDef, _ Deps) (any, error) { return 0, nil },
		register:    em.Register,
		deregister:  em.Deregister,
		suspendedCh: em.Suspended,
		isSuspend:   func(error) bool { return false },
	})
	_, reason3, susp3 := runSchedTimeout(t, sc3, 2*time.Second)
	if susp3 {
		t.Fatal("real-runtime: spurious suspension on inverted-order synchronous skip")
	}
	if reason3 != AllCompleted {
		t.Fatalf("real-runtime: reason=%v want AllCompleted", reason3)
	}
}
