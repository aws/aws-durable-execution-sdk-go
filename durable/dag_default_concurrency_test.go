package durable_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// peakTracker records the maximum number of task bodies observed running
// concurrently, using sync/atomic so it is a genuine OBSERVED peak of live
// goroutines rather than a restatement of the configured bound. enter() is
// called on task entry and exit() on task exit (deferred).
type peakTracker struct {
	cur  int64
	peak int64
}

func (p *peakTracker) enter() {
	n := atomic.AddInt64(&p.cur, 1)
	for {
		old := atomic.LoadInt64(&p.peak)
		if n <= old || atomic.CompareAndSwapInt64(&p.peak, old, n) {
			break
		}
	}
}

func (p *peakTracker) exit()      { atomic.AddInt64(&p.cur, -1) }
func (p *peakTracker) max() int64 { return atomic.LoadInt64(&p.peak) }

// registerWideGraph registers nTasks independent (dependency-free) step
// tasks that each mark their concurrent presence on tr and hold for hold so
// the scheduler saturates its bound before any task frees a slot.
func registerWideGraph(d *durable.DagBuilder, nTasks int, hold time.Duration, tr *peakTracker) {
	for i := 0; i < nTasks; i++ {
		durable.DagStep(d, fmt.Sprintf("t_%d", i), nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) {
				tr.enter()
				defer tr.exit()
				time.Sleep(hold)
				return 0, nil
			})
	}
}

// TestDagE2E_DefaultConcurrencyCapsWideGraph is the sensitive guard for the
// default-concurrency contract (review finding H2): a DAG whose config does
// NOT set WithDagMaxConcurrency runs at most DefaultDagMaxConcurrency (40)
// top-level tasks concurrently, where it was previously unbounded.
//
// The graph is 100 independent root tasks (> 40) with no concurrency option.
// The assertion is on the OBSERVED peak of live task goroutines captured via
// sync/atomic — NOT the configured value — so a regression that dropped the
// default back to unbounded would drive the observed peak toward 100 and
// fail here.
func TestDagE2E_DefaultConcurrencyCapsWideGraph(t *testing.T) {
	const nTasks = 100
	var tr peakTracker

	type out struct {
		Peak    int64
		Success int
		Reason  string
	}
	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "widedag", func(d *durable.DagBuilder) {
			registerWideGraph(d, nTasks, 40*time.Millisecond, &tr)
		})
		if err != nil {
			return out{}, err
		}
		if e := res.ThrowIfError(); e != nil {
			return out{}, e
		}
		return out{Peak: tr.max(), Success: res.SucceededCount(), Reason: string(res.CompletionReason())}, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[out](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}

	// (1) THE sensitive invariant: a graph wider than 40 never exceeds 40
	// concurrent task goroutines under the default.
	if o.Peak > durable.DefaultDagMaxConcurrency {
		t.Fatalf("observed peak concurrency=%d exceeded the default bound of %d (unbounded regression?)",
			o.Peak, durable.DefaultDagMaxConcurrency)
	}
	// (2) the default genuinely BINDS (the graph is wider than 40, so it
	// must actually reach the cap rather than trivially staying under it).
	if o.Peak != durable.DefaultDagMaxConcurrency {
		t.Fatalf("observed peak concurrency=%d, want exactly %d (the default should bind on a %d-task graph)",
			o.Peak, durable.DefaultDagMaxConcurrency, nTasks)
	}
	// (3) capping only changes overlap, never outcome: all tasks succeed.
	if o.Success != nTasks || o.Reason != string(durable.AllCompleted) {
		t.Fatalf("unexpected aggregate: success=%d reason=%q (want %d / AllCompleted)", o.Success, o.Reason, nTasks)
	}
}

// TestDagE2E_ExplicitConcurrencyBelowDefaultWins asserts an explicit bound
// below 40 still wins (the default does not override a smaller explicit
// value).
func TestDagE2E_ExplicitConcurrencyBelowDefaultWins(t *testing.T) {
	const nTasks = 100
	const limit = 5
	var tr peakTracker

	handler := func(dc durable.Context, _ struct{}) (int64, error) {
		res, err := durable.Dag(dc, "belowdag", func(d *durable.DagBuilder) {
			registerWideGraph(d, nTasks, 20*time.Millisecond, &tr)
		}, durable.WithDagMaxConcurrency(limit))
		if err != nil {
			return 0, err
		}
		if e := res.ThrowIfError(); e != nil {
			return 0, e
		}
		return tr.max(), nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	peak, err := durabletest.ResultAs[int64](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if peak > limit {
		t.Fatalf("observed peak=%d exceeded explicit limit %d", peak, limit)
	}
	if peak != limit {
		t.Fatalf("observed peak=%d, want exactly %d (explicit bound below the default should bind)", peak, limit)
	}
}

// TestDagE2E_ExplicitConcurrencyAboveDefaultWins asserts an explicit bound
// ABOVE 40 still wins — the default must not silently clamp a larger
// explicit value down to 40.
func TestDagE2E_ExplicitConcurrencyAboveDefaultWins(t *testing.T) {
	const nTasks = 100
	const limit = 64
	var tr peakTracker

	handler := func(dc durable.Context, _ struct{}) (int64, error) {
		res, err := durable.Dag(dc, "abovedag", func(d *durable.DagBuilder) {
			registerWideGraph(d, nTasks, 40*time.Millisecond, &tr)
		}, durable.WithDagMaxConcurrency(limit))
		if err != nil {
			return 0, err
		}
		if e := res.ThrowIfError(); e != nil {
			return 0, e
		}
		return tr.max(), nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	peak, err := durabletest.ResultAs[int64](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if peak > limit {
		t.Fatalf("observed peak=%d exceeded explicit limit %d", peak, limit)
	}
	// The point of this test: the explicit value above 40 is honored, i.e.
	// the DAG runs MORE than the default 40 concurrently.
	if peak <= durable.DefaultDagMaxConcurrency {
		t.Fatalf("observed peak=%d did not exceed the default %d; an explicit bound above 40 was not honored",
			peak, durable.DefaultDagMaxConcurrency)
	}
}

// TestDagE2E_ExplicitZeroConcurrencyIsUnbounded asserts the explicit
// unbounded sentinel: WithDagMaxConcurrency(0) removes the default-40 cap
// entirely, matching Map/Parallel's own unbounded-by-omission default. This
// is the fix for the Go-idiom gap identified in the language review --
// previously there was no way to opt back into unbounded top-level
// concurrency short of passing an arbitrarily large bound.
func TestDagE2E_ExplicitZeroConcurrencyIsUnbounded(t *testing.T) {
	const nTasks = 100
	var tr peakTracker

	handler := func(dc durable.Context, _ struct{}) (int64, error) {
		res, err := durable.Dag(dc, "unboundeddag", func(d *durable.DagBuilder) {
			registerWideGraph(d, nTasks, 40*time.Millisecond, &tr)
		}, durable.WithDagMaxConcurrency(0))
		if err != nil {
			return 0, err
		}
		if e := res.ThrowIfError(); e != nil {
			return 0, e
		}
		return tr.max(), nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	peak, err := durabletest.ResultAs[int64](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	// The point of this test: an explicit 0 lifts the cap entirely, so all
	// 100 independent tasks run concurrently -- well above the default of 40.
	if peak <= durable.DefaultDagMaxConcurrency {
		t.Fatalf("observed peak=%d did not exceed the default %d; WithDagMaxConcurrency(0) did not remove the cap",
			peak, durable.DefaultDagMaxConcurrency)
	}
}

// TestDagE2E_NestedDagIndependentDefault asserts a nested SubDag gets its
// OWN independent default of 40: the parent DAG sets no bound (and holds only
// one top-level task, the SubDag), and the nested DAG's wide inner graph is
// capped at 40 on its own, per the contract's nesting rule.
func TestDagE2E_NestedDagIndependentDefault(t *testing.T) {
	const innerTasks = 100
	var tr peakTracker

	handler := func(dc durable.Context, _ struct{}) (int64, error) {
		res, err := durable.Dag(dc, "outerdag", func(d *durable.DagBuilder) {
			durable.SubDag(d, "inner", nil, func(sub *durable.DagBuilder) {
				registerWideGraph(sub, innerTasks, 40*time.Millisecond, &tr)
			})
		})
		if err != nil {
			return 0, err
		}
		if e := res.ThrowIfError(); e != nil {
			return 0, e
		}
		return tr.max(), nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	peak, err := durabletest.ResultAs[int64](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if peak > durable.DefaultDagMaxConcurrency {
		t.Fatalf("nested DAG observed peak=%d exceeded its independent default of %d",
			peak, durable.DefaultDagMaxConcurrency)
	}
	if peak != durable.DefaultDagMaxConcurrency {
		t.Fatalf("nested DAG observed peak=%d, want exactly %d (nested default should bind on a %d-task inner graph)",
			peak, durable.DefaultDagMaxConcurrency, innerTasks)
	}
}
