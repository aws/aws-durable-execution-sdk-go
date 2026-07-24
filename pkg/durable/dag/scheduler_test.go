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
