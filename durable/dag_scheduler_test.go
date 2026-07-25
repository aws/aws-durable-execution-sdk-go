package durable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runSched builds a scheduler over d's tasks with a fake runner and runs it.
func runSched(d *DagBuilder, maxConc int, completion *DagCompletionConfig, runTask func(def *dagTaskDef, deps Deps) (any, error)) ([]TaskExecution, DagCompletionReason) {
	s := newDagScheduler(d.tasks, maxConc, completion, dagSchedHooks{runTask: runTask})
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

func TestDagScheduler_TopologicalOrder(t *testing.T) {
	d := newDagBuilder()
	fetch := DagStep(d, "fetch", nil, func(_ Deps, _ StepContext) (int, error) { return 1, nil })
	a := DagStep(d, "a", []AnyHandle{fetch}, func(_ Deps, _ StepContext) (int, error) { return 2, nil })
	b := DagStep(d, "b", []AnyHandle{fetch}, func(_ Deps, _ StepContext) (int, error) { return 3, nil })
	DagStep(d, "merge", []AnyHandle{a, b}, func(_ Deps, _ StepContext) (int, error) { return 4, nil })

	var mu sync.Mutex
	order := []string{}
	execs, reason := runSched(d, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) {
		mu.Lock()
		order = append(order, def.name)
		mu.Unlock()
		return 0, nil
	})

	if reason != AllCompleted {
		t.Fatalf("reason=%v", reason)
	}
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

func TestDagScheduler_PeakConcurrencyBounded(t *testing.T) {
	d := newDagBuilder()
	for i := 0; i < 20; i++ {
		DagStep(d, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	var cur, peak int64
	limit := 4
	runSched(d, limit, nil, func(def *dagTaskDef, _ Deps) (any, error) {
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

func TestDagScheduler_SkipCascade(t *testing.T) {
	d := newDagBuilder()
	root := DagStep(d, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, errors.New("boom") })
	child := DagStep(d, "child", []AnyHandle{root}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "grandchild", []AnyHandle{child}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

	execs, reason := runSched(d, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) {
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

func TestDagScheduler_RunIfSkip(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	execs, _ := runSched(d, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) { return 0, nil })
	st := statusByName(execs)
	if st["a"] != StatusSkipped {
		t.Fatalf("runIf=false should skip, got %v", st)
	}
	if execs[0].SkipReason != SkipRunIf {
		t.Fatalf("skip reason=%v", execs[0].SkipReason)
	}
}

func TestDagScheduler_TriggerAllFailedRunsCompensation(t *testing.T) {
	d := newDagBuilder()
	charge := DagStep(d, "charge", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "refund", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }).
		After(charge).WithTrigger(AllFailed)
	ran := map[string]bool{}
	var mu sync.Mutex
	execs, _ := runSched(d, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) {
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

func TestDagScheduler_ThresholdMinSuccessful(t *testing.T) {
	d := newDagBuilder()
	for i := 0; i < 5; i++ {
		DagStep(d, fmt.Sprintf("r%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	minv := 2
	_, reason := runSched(d, 1, &DagCompletionConfig{MinSuccessful: &minv}, func(def *dagTaskDef, _ Deps) (any, error) {
		return 0, nil
	})
	if reason != MinSuccessfulReached {
		t.Fatalf("reason=%v want MinSuccessfulReached", reason)
	}
}

func TestDagScheduler_CustomCompletion(t *testing.T) {
	d := newDagBuilder()
	for i := 0; i < 5; i++ {
		DagStep(d, fmt.Sprintf("rule%d", i), nil, func(_ Deps, _ StepContext) (string, error) { return "OK", nil })
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
	_, reason := runSched(d, 2, completion, func(def *dagTaskDef, _ Deps) (any, error) { return "OK", nil })
	if reason != AllCompleted {
		t.Fatalf("reason=%v want AllCompleted (no reject)", reason)
	}

	// One REJECT -> custom failed.
	_, reason2 := runSched(d, 1, completion, func(def *dagTaskDef, _ Deps) (any, error) {
		if def.name == "rule2" {
			return "REJECT", nil
		}
		return "OK", nil
	})
	if reason2 != CustomCompletionFailed {
		t.Fatalf("reason=%v want CustomCompletionFailed", reason2)
	}
}

func TestDagScheduler_DrainByDefaultDoesNotCancelSiblings(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "fails", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "ok", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	execs, reason := runSched(d, 2, nil, func(def *dagTaskDef, _ Deps) (any, error) {
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

func TestDagScheduler_DepValuesFlowThroughDeps(t *testing.T) {
	d := newDagBuilder()
	src := DagStep(d, "src", nil, func(_ Deps, _ StepContext) (int, error) { return 21, nil })
	DagStep(d, "dbl", []AnyHandle{src}, func(deps Deps, _ StepContext) (int, error) {
		v, err := Get(deps, src)
		if err != nil {
			return 0, err
		}
		return v * 2, nil
	})
	execs, _ := runSched(d, 0, nil, func(def *dagTaskDef, deps Deps) (any, error) {
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

// TestDagScheduler_DefaultTriggerRuleApplied verifies a DAG-level default
// trigger rule is honored for tasks that set none of their own.
func TestDagScheduler_DefaultTriggerRuleApplied(t *testing.T) {
	d := newDagBuilder()
	root := DagStep(d, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "comp", []AnyHandle{root}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

	s := newDagScheduler(d.tasks, 0, nil, dagSchedHooks{runTask: func(def *dagTaskDef, _ Deps) (any, error) {
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

	// Control: with the built-in default (AllSuccess), the same graph skips "comp".
	d2 := newDagBuilder()
	root2 := DagStep(d2, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d2, "comp", []AnyHandle{root2}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	execs2, _ := runSched(d2, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) {
		if def.name == "root" {
			return 0, errors.New("boom")
		}
		return 0, nil
	})
	if statusByName(execs2)["comp"] != StatusSkipped {
		t.Fatalf("control: comp should skip under AllSuccess default")
	}
}

// TestDagScheduler_EmptyDag verifies an empty graph drains cleanly.
func TestDagScheduler_EmptyDag(t *testing.T) {
	d := newDagBuilder()
	execs, reason := runSched(d, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) { return nil, nil })
	if len(execs) != 0 {
		t.Fatalf("empty dag should have 0 execs, got %d", len(execs))
	}
	if reason != AllCompleted {
		t.Fatalf("empty dag reason=%v want AllCompleted", reason)
	}
}

// raceExecMgr is a faithful in-test model of the execmgr active-goroutine
// accounting and Suspended-channel semantics, so the scheduler's
// park/hand-off protocol is exercised against a real suspension signal.
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

// TestDagScheduler_NoSpuriousSuspension is the regression guard for the
// park+hand-off BLOCKER: a bounded-concurrency DAG whose tasks all complete
// normally (never suspend), run many times against a real execmgr model,
// must NEVER report suspension.
func TestDagScheduler_NoSpuriousSuspension(t *testing.T) {
	const iters = 3000
	const n = 6
	for it := 0; it < iters; it++ {
		d := newDagBuilder()
		for i := 0; i < n; i++ {
			DagStep(d, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
		}
		em := newRaceExecMgr()
		em.Register() // parent (handler) goroutine enters run() registered
		hooks := dagSchedHooks{
			runTask: func(_ *dagTaskDef, _ Deps) (any, error) {
				time.Sleep(3 * time.Microsecond)
				return 0, nil
			},
			register:    em.Register,
			deregister:  em.Deregister,
			suspendedCh: em.Suspended,
			isSuspend:   func(error) bool { return false },
		}
		s := newDagScheduler(d.tasks, 2, nil, hooks)
		execs, _, susp := s.run(context.Background())
		if susp {
			t.Fatalf("iteration %d: spurious suspension for a DAG whose tasks all completed", it)
		}
		if len(execs) != n {
			t.Fatalf("iteration %d: want %d execs, got %d", it, n, len(execs))
		}
	}
}

// TestDagScheduler_ThresholdFailurePercentage guards the
// ToleratedFailurePercentage denominator being the fixed task count, not the
// settled-so-far count.
func TestDagScheduler_ThresholdFailurePercentage(t *testing.T) {
	pct := 50.0

	// Only the first task fails => 25% of 4 => under tolerance => full drain.
	d := newDagBuilder()
	for i := 0; i < 4; i++ {
		DagStep(d, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	execs, reason := runSched(d, 1, &DagCompletionConfig{ToleratedFailurePercentage: &pct}, func(def *dagTaskDef, _ Deps) (any, error) {
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
	d2 := newDagBuilder()
	for i := 0; i < 4; i++ {
		DagStep(d2, fmt.Sprintf("t%d", i), nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	_, reason2 := runSched(d2, 1, &DagCompletionConfig{ToleratedFailurePercentage: &pct}, func(def *dagTaskDef, _ Deps) (any, error) {
		return 0, errors.New("boom")
	})
	if reason2 != FailureToleranceExceeded {
		t.Fatalf("majority failures should trip 50%% tolerance; reason=%v", reason2)
	}
}

// TestDagScheduler_CustomCompletionAfterSkip guards that a custom
// ShouldComplete predicate is evaluated after a SKIP settles.
func TestDagScheduler_CustomCompletionAfterSkip(t *testing.T) {
	d := newDagBuilder()
	root := DagStep(d, "root", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "child", []AnyHandle{root}, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

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
	_, reason := runSched(d, 1, completion, func(def *dagTaskDef, _ Deps) (any, error) {
		if def.name == "root" {
			return 0, errors.New("boom")
		}
		return 0, nil
	})
	if reason != CustomCompletionFailed {
		t.Fatalf("custom predicate must fire on a SKIP settle; reason=%v want CustomCompletionFailed", reason)
	}
}

// runSchedTimeout runs the scheduler but fails the test (rather than hanging)
// if it does not return within d — the park BLOCKER manifests as a deadlock.
func runSchedTimeout(t *testing.T, sc *dagScheduler, d time.Duration) ([]TaskExecution, DagCompletionReason, bool) {
	t.Helper()
	type res struct {
		execs  []TaskExecution
		reason DagCompletionReason
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

// TestDagScheduler_SkipFreesEarlierDependent is the regression guard for the
// park BLOCKER: a synchronous skip resolved in one startReady pass can free a
// dependent that appears EARLIER in registration order (legal via After) and
// so is not re-scanned that pass. With zero tasks in-flight the parent must
// NOT park.
func TestDagScheduler_SkipFreesEarlierDependent(t *testing.T) {
	// single upstream: y registered BEFORE x; y depends on x; x skipped.
	d := newDagBuilder()
	y := DagStep(d, "y", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	x := DagStep(d, "x", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	y.After(x)

	sc := newDagScheduler(d.tasks, 0, nil, dagSchedHooks{
		runTask:   func(def *dagTaskDef, _ Deps) (any, error) { return 0, nil },
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

	// multi upstream: y registered BEFORE x1,x2; both skipped in same pass.
	d2 := newDagBuilder()
	y2 := DagStep(d2, "y", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	x1 := DagStep(d2, "x1", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	x2 := DagStep(d2, "x2", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	y2.After(x1, x2)

	sc2 := newDagScheduler(d2.tasks, 0, nil, dagSchedHooks{
		runTask:   func(def *dagTaskDef, _ Deps) (any, error) { return 0, nil },
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

	// real-runtime model: same inverted-order skip must not spuriously suspend.
	d3 := newDagBuilder()
	y3 := DagStep(d3, "y", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	x3 := DagStep(d3, "x", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithRunIf(func(Deps) bool { return false }))
	y3.After(x3)
	em := newRaceExecMgr()
	em.Register()
	sc3 := newDagScheduler(d3.tasks, 0, nil, dagSchedHooks{
		runTask:     func(def *dagTaskDef, _ Deps) (any, error) { return 0, nil },
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

// TestDagScheduler_WorkerPanicFailsTaskNotProcess exercises the
// goroutine-entry recover directly: a runTask that panics must be turned into
// a task failure (delivered through the normal deliverDone path) so the
// panicking task is FAILED, siblings still complete, and the DAG drains with
// COMPLETED_WITH_FAILURES — never a process crash. Uses the fake runner, so
// failTask is unset and the recover falls back to the bare cause.
func TestDagScheduler_WorkerPanicFailsTaskNotProcess(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "boom", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "ok", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

	execs, reason := runSched(d, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) {
		if def.name == "boom" {
			panic("worker boom")
		}
		return 0, nil
	})
	st := statusByName(execs)
	if st["boom"] != StatusFailed {
		t.Fatalf("panicking worker task should be FAILED, got %v", st["boom"])
	}
	if st["ok"] != StatusSucceeded {
		t.Fatalf("sibling should still complete, got %v", st["ok"])
	}
	if reason != CompletedWithFailures {
		t.Fatalf("reason=%v want CompletedWithFailures", reason)
	}
	for _, e := range execs {
		if e.Name == "boom" && (e.Err == nil || !strings.Contains(e.Err.Error(), "panicked")) {
			t.Fatalf("boom.Err should describe the panic, got %v", e.Err)
		}
	}
}

// TestDagScheduler_RunIfPanicFailsTaskNotProcess exercises the synchronous
// runIf recover: a panicking predicate fails only that task (the task never
// starts) and the graph still drains.
func TestDagScheduler_RunIfPanicFailsTaskNotProcess(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "boom", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil },
		WithRunIf(func(Deps) bool { panic("runIf boom") }))
	DagStep(d, "ok", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })

	execs, reason := runSched(d, 0, nil, func(def *dagTaskDef, _ Deps) (any, error) { return 0, nil })
	st := statusByName(execs)
	if st["boom"] != StatusFailed {
		t.Fatalf("task with panicking runIf should be FAILED, got %v", st["boom"])
	}
	if st["ok"] != StatusSucceeded {
		t.Fatalf("sibling should still complete, got %v", st["ok"])
	}
	if reason != CompletedWithFailures {
		t.Fatalf("reason=%v want CompletedWithFailures", reason)
	}
	for _, e := range execs {
		if e.Name == "boom" && (e.Err == nil || !strings.Contains(e.Err.Error(), "runIf panicked")) {
			t.Fatalf("boom.Err should describe the runIf panic, got %v", e.Err)
		}
	}
}
