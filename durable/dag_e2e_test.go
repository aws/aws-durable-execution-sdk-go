package durable_test

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

type e2eDagOut struct {
	Merged  int
	Success int
	Failure int
	Skipped int
	Reason  string
}

// TestDagE2E_DiamondSucceeds runs fetch -> {a,b} -> merge end to end on the
// local runner and asserts the aggregate result.
func TestDagE2E_DiamondSucceeds(t *testing.T) {
	var fetchRuns, mergeRuns int32

	handler := func(dc durable.Context, _ struct{}) (e2eDagOut, error) {
		res, err := durable.Dag(dc, "etl", func(d *durable.DagBuilder) {
			fetch := durable.DagStep(d, "fetch", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				atomic.AddInt32(&fetchRuns, 1)
				return 10, nil
			})
			a := durable.DagStep(d, "a", []durable.AnyHandle{fetch}, func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, _ := durable.Get(deps, fetch)
				return v + 1, nil
			})
			b := durable.DagStep(d, "b", []durable.AnyHandle{fetch}, func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, _ := durable.Get(deps, fetch)
				return v + 2, nil
			})
			durable.DagStep(d, "merge", []durable.AnyHandle{a, b}, func(deps durable.Deps, _ durable.StepContext) (int, error) {
				atomic.AddInt32(&mergeRuns, 1)
				av, _ := durable.Get(deps, a)
				bv, _ := durable.Get(deps, b)
				return av + bv, nil
			})
		})
		if err != nil {
			return e2eDagOut{}, err
		}
		if e := res.ThrowIfError(); e != nil {
			return e2eDagOut{}, e
		}
		merge, _ := durable.ResultByName[int](res, "merge")
		return e2eDagOut{Merged: merge, Success: res.SucceededCount(), Failure: res.FailureCount(), Skipped: res.SkippedCount(), Reason: string(res.CompletionReason())}, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	out, err := durabletest.ResultAs[e2eDagOut](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if out.Merged != 23 { // a=11, b=12 => merge=23
		t.Fatalf("merge=%d want 23", out.Merged)
	}
	if out.Success != 4 || out.Failure != 0 || out.Reason != string(durable.AllCompleted) {
		t.Fatalf("unexpected aggregate: %+v", out)
	}
}

// TestDagE2E_TaskFailureReportedInResult asserts a task failure surfaces via
// res.ThrowIfError() (not the Dag error return) and cascades skips.
func TestDagE2E_TaskFailureReportedInResult(t *testing.T) {
	type out struct {
		DagErr   bool
		Failures int
		Skips    int
		Reason   string
	}
	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "wf", func(d *durable.DagBuilder) {
			root := durable.DagStep(d, "root", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				return 0, errors.New("boom")
			})
			durable.DagStep(d, "downstream", []durable.AnyHandle{root}, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				return 1, nil
			})
		})
		if err != nil {
			return out{}, err
		}
		return out{DagErr: res.ThrowIfError() != nil, Failures: res.FailureCount(), Skips: res.SkippedCount(), Reason: string(res.CompletionReason())}, nil
	}
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("handler should succeed (task failures are in-result), got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[out](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if !o.DagErr || o.Failures != 1 || o.Skips != 1 || o.Reason != string(durable.CompletedWithFailures) {
		t.Fatalf("unexpected: %+v", o)
	}
}

// TestDagE2E_WaitSuspendResume exercises the suspend protocol: a Wait task
// between two steps suspends the invocation, and the runner drives it to
// completion across resume, with side effects counted once.
func TestDagE2E_WaitSuspendResume(t *testing.T) {
	var beforeRuns, afterRuns int32
	handler := func(dc durable.Context, _ struct{}) (string, error) {
		res, err := durable.Dag(dc, "timed", func(d *durable.DagBuilder) {
			before := durable.DagStep(d, "before", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				atomic.AddInt32(&beforeRuns, 1)
				return "b", nil
			})
			pause := durable.DagWait(d, "pause", []durable.AnyHandle{before}, 5*time.Second)
			durable.DagStep(d, "after", []durable.AnyHandle{pause}, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				atomic.AddInt32(&afterRuns, 1)
				return "a", nil
			})
		})
		if err != nil {
			return "", err
		}
		if e := res.ThrowIfError(); e != nil {
			return "", e
		}
		return string(res.CompletionReason()), nil
	}
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED after wait resolves, got %s (%+v)", result.Status, result.Error)
	}
	if beforeRuns != 1 || afterRuns != 1 {
		t.Fatalf("side effects not once: before=%d after=%d", beforeRuns, afterRuns)
	}
}

// TestDagE2E_CustomCompletion exercises a results-aware custom predicate that
// fails the DAG when any rule returns REJECT.
func TestDagE2E_CustomCompletion(t *testing.T) {
	handler := func(dc durable.Context, reject bool) (string, error) {
		res, err := durable.Dag(dc, "rules", func(d *durable.DagBuilder) {
			var prev durable.AnyHandle
			for i := 0; i < 4; i++ {
				i := i
				var deps []durable.AnyHandle
				if prev != nil {
					deps = []durable.AnyHandle{prev}
				}
				prev = durable.DagStep(d, e2eRuleName(i), deps, func(_ durable.Deps, _ durable.StepContext) (string, error) {
					if reject && i == 2 {
						return "REJECT", nil
					}
					return "OK", nil
				})
			}
		},
			durable.WithDagMaxConcurrency(1),
			durable.WithDagCompletion(durable.DagCompletionConfig{
				ShouldComplete: func(st durable.DagCompletionStatus) durable.CompletionDecision {
					for _, it := range st.Items {
						if it.Status == durable.StatusSucceeded {
							if v, ok := durable.ResultOf[string](it); ok && v == "REJECT" {
								return durable.CompleteDag(durable.OutcomeFailed)
							}
						}
					}
					return durable.ContinueDag()
				},
			}),
		)
		if err != nil {
			return "", err
		}
		return string(res.CompletionReason()), nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, true)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected handler SUCCEEDED, got %s (%+v)", result.Status, result.Error)
	}
	reason, err := durabletest.ResultAs[string](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if reason != string(durable.CustomCompletionFailed) {
		t.Fatalf("reason=%q want CUSTOM_COMPLETION_FAILED", reason)
	}
}

func e2eRuleName(i int) string { return "rule_" + string(rune('0'+i)) }

// TestDagE2E_DefaultRetryApplied proves the DAG-level default retry
// (WithDefaultRetry) is wired to tasks that set none of their own: a flaky
// step fails once, then the default retry recovers it.
func TestDagE2E_DefaultRetryApplied(t *testing.T) {
	var attempts int32
	handler := func(dc durable.Context, _ struct{}) (string, error) {
		res, err := durable.Dag(dc, "retrying", func(d *durable.DagBuilder) {
			durable.DagStep(d, "flaky", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				if atomic.AddInt32(&attempts, 1) == 1 {
					return "", errors.New("transient failure")
				}
				return "ok", nil
			})
		}, durable.WithDefaultRetry(func(_ error, attempt int) durable.RetryDecision {
			return durable.RetryDecision{Retry: attempt < 2, Delay: 0}
		}))
		if err != nil {
			return "", err
		}
		if e := res.ThrowIfError(); e != nil {
			return "", e
		}
		return string(res.CompletionReason()), nil
	}
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected SUCCEEDED (default retry should recover flaky step), got %s (%+v)", result.Status, result.Error)
	}
	if atomic.LoadInt32(&attempts) < 2 {
		t.Fatalf("default retry not applied: attempts=%d (want >=2)", atomic.LoadInt32(&attempts))
	}
}

// TestDagE2E_LargePayloadReplay builds a DAG whose big per-task results are
// reconstructed intact across a suspend/resume (a Wait gated behind them),
// and asserts completed task bodies are not re-executed on replay.
func TestDagE2E_LargePayloadReplay(t *testing.T) {
	const chunkBytes = 64 * 1024
	const bigTaskCount = 6
	var runs [bigTaskCount]int32

	type out struct {
		TotalLen int
		Success  int
		Reason   string
	}

	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "bigpayload", func(d *durable.DagBuilder) {
			seed := durable.DagStep(d, "seed", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				return chunkBytes, nil
			})
			bigs := make([]durable.AnyHandle, 0, bigTaskCount)
			for i := 0; i < bigTaskCount; i++ {
				i := i
				h := durable.DagStep(d, e2eBigName(i), []durable.AnyHandle{seed}, func(deps durable.Deps, _ durable.StepContext) (string, error) {
					atomic.AddInt32(&runs[i], 1)
					n, _ := durable.Get(deps, seed)
					return strings.Repeat("x", n), nil
				})
				bigs = append(bigs, h)
			}
			durable.DagWait(d, "pause", bigs, 5*time.Second)
		})
		if err != nil {
			return out{}, err
		}
		if e := res.ThrowIfError(); e != nil {
			return out{}, e
		}
		total := 0
		for i := 0; i < bigTaskCount; i++ {
			v, rerr := durable.ResultByName[string](res, e2eBigName(i))
			if rerr != nil {
				return out{}, rerr
			}
			total += len(v)
		}
		return out{TotalLen: total, Success: res.SucceededCount(), Reason: string(res.CompletionReason())}, nil
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
	if want := bigTaskCount * chunkBytes; o.TotalLen != want {
		t.Fatalf("reconstructed payload length=%d want %d", o.TotalLen, want)
	}
	if want := bigTaskCount + 2; o.Success != want { // seed + big tasks + pause wait
		t.Fatalf("success count=%d want %d", o.Success, want)
	}
	if o.Reason != string(durable.AllCompleted) {
		t.Fatalf("reason=%q want AllCompleted", o.Reason)
	}
	for i := 0; i < bigTaskCount; i++ {
		if got := atomic.LoadInt32(&runs[i]); got != 1 {
			t.Fatalf("task %s body ran %d times, want 1 (checkpoint fast-path not hit on replay)", e2eBigName(i), got)
		}
	}
}

func e2eBigName(i int) string { return "big_" + string(rune('0'+i)) }

// TestDagE2E_OrderIndependenceReplay runs the same independent-branch diamond
// twice, forcing the two branches to settle in opposite orders, and asserts
// the resulting DagResult is identical.
func TestDagE2E_OrderIndependenceReplay(t *testing.T) {
	type out struct {
		Merged  int
		Success int
		Failure int
		Reason  string
	}

	runDiamond := func(aFirst bool) (out, error) {
		signal := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(signal) }) }

		handler := func(dc durable.Context, _ struct{}) (out, error) {
			res, derr := durable.Dag(dc, "diamond", func(d *durable.DagBuilder) {
				fetch := durable.DagStep(d, "fetch", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
					return 10, nil
				})
				a := durable.DagStep(d, "a", []durable.AnyHandle{fetch}, func(deps durable.Deps, _ durable.StepContext) (int, error) {
					if aFirst {
						release()
					} else {
						<-signal
					}
					v, _ := durable.Get(deps, fetch)
					return v + 1, nil
				})
				b := durable.DagStep(d, "b", []durable.AnyHandle{fetch}, func(deps durable.Deps, _ durable.StepContext) (int, error) {
					if aFirst {
						<-signal
					} else {
						release()
					}
					v, _ := durable.Get(deps, fetch)
					return v + 2, nil
				})
				durable.DagStep(d, "merge", []durable.AnyHandle{a, b}, func(deps durable.Deps, _ durable.StepContext) (int, error) {
					av, _ := durable.Get(deps, a)
					bv, _ := durable.Get(deps, b)
					return av + bv, nil
				})
			})
			if derr != nil {
				return out{}, derr
			}
			if e := res.ThrowIfError(); e != nil {
				return out{}, e
			}
			merge, _ := durable.ResultByName[int](res, "merge")
			return out{Merged: merge, Success: res.SucceededCount(), Failure: res.FailureCount(), Reason: string(res.CompletionReason())}, nil
		}

		runner := durabletest.NewLocalRunner(handler)
		result := runner.RunUntilComplete(t, struct{}{})
		if result.Status != durabletest.Succeeded {
			t.Fatalf("expected SUCCEEDED (aFirst=%v), got %s (%+v)", aFirst, result.Status, result.Error)
		}
		return durabletest.ResultAs[out](result)
	}

	aThenB, err := runDiamond(true)
	if err != nil {
		t.Fatalf("run aFirst: %v", err)
	}
	bThenA, err := runDiamond(false)
	if err != nil {
		t.Fatalf("run bFirst: %v", err)
	}
	if aThenB != bThenA {
		t.Fatalf("order-dependent result: aFirst=%+v bFirst=%+v", aThenB, bThenA)
	}
	if aThenB.Merged != 23 || aThenB.Success != 4 || aThenB.Failure != 0 || aThenB.Reason != string(durable.AllCompleted) {
		t.Fatalf("unexpected diamond result: %+v", aThenB)
	}
}

// TestDagE2E_PanicInTaskIsContainedAsFailure is a regression guard for
// BLOCKER B2. A panic in user code reachable from the scheduler worker
// goroutine but OUTSIDE any core-op recover — here a DagMap items() func,
// which runs in the worker goroutine before Map() is entered — must NOT
// crash the process. It must be converted into a clean task failure so the
// panicking task is FAILED, siblings still complete, and the DAG drains with
// COMPLETED_WITH_FAILURES. Before the fix this aborted the test binary.
func TestDagE2E_PanicInTaskIsContainedAsFailure(t *testing.T) {
	type out struct {
		DagErr     bool
		ErrMsg     string
		BoomStatus string
		OkStatus   string
		Failures   int
		Success    int
		Reason     string
	}
	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "wf", func(d *durable.DagBuilder) {
			durable.DagMap(d, "boom", nil,
				func(_ durable.Deps) []int { panic("items exploded") },
				func(_ durable.Context, item int, _ int) (int, error) { return item, nil })
			durable.DagStep(d, "ok", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				return 1, nil
			})
		})
		if err != nil {
			return out{}, err
		}
		boom, _ := res.Status("boom")
		ok, _ := res.Status("ok")
		o := out{
			DagErr:     res.ThrowIfError() != nil,
			BoomStatus: string(boom),
			OkStatus:   string(ok),
			Failures:   res.FailureCount(),
			Success:    res.SucceededCount(),
			Reason:     string(res.CompletionReason()),
		}
		if e := res.ThrowIfError(); e != nil {
			o.ErrMsg = e.Error()
		}
		return o, nil
	}
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("handler should succeed (panic contained as task failure, process alive), got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[out](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if o.BoomStatus != string(durable.StatusFailed) {
		t.Fatalf("panicking task should be FAILED, got %q", o.BoomStatus)
	}
	if o.OkStatus != string(durable.StatusSucceeded) {
		t.Fatalf("sibling should still complete, got %q", o.OkStatus)
	}
	if !o.DagErr || o.Failures != 1 || o.Success != 1 || o.Reason != string(durable.CompletedWithFailures) {
		t.Fatalf("panic not contained as a single task failure: %+v", o)
	}
	if !strings.Contains(o.ErrMsg, "panicked") {
		t.Fatalf("task-failure error should carry the panic; got %q", o.ErrMsg)
	}
}

// TestDagE2E_PanicInRunIfAbortsDag is the end-to-end guard for the runIf
// abort contract (review finding H5). runIf is evaluated on the scheduler
// goroutine (not a worker), so it is not covered by the worker recover; a
// panic there must be recovered (never reach the runtime) but then ABORT the
// DAG with a typed *DagPredicateError rather than be recorded as a task
// failure. The process survives, Dag returns the typed error and no
// DagResult, the container checkpoints a failure, and the downstream
// ALL_FAILED compensation task never runs.
func TestDagE2E_PanicInRunIfAbortsDag(t *testing.T) {
	var refundRuns int32
	type out struct {
		HadErr     bool
		IsPredErr  bool
		PredTask   string
		ResNil     bool
		ErrMsg     string
		RefundRuns int32
	}
	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "wf", func(d *durable.DagBuilder) {
			boom := durable.DagStep(d, "boom", nil,
				func(_ durable.Deps, _ durable.StepContext) (int, error) { return 0, nil },
				durable.WithRunIf(func(durable.Deps) bool { panic("runIf exploded") }))
			// Downstream ALL_FAILED compensation: under the OLD task-failure
			// semantics boom would be FAILED and this would fire (a defect
			// issuing a refund). Under the abort contract it must never run.
			durable.DagStep(d, "refund", []durable.AnyHandle{boom},
				func(_ durable.Deps, _ durable.StepContext) (int, error) {
					atomic.AddInt32(&refundRuns, 1)
					return 0, nil
				}, durable.WithTriggerRule(durable.AllFailed))
		})
		var pe *durable.DagPredicateError
		o := out{
			HadErr:     err != nil,
			IsPredErr:  errors.As(err, &pe),
			ResNil:     res == nil,
			RefundRuns: atomic.LoadInt32(&refundRuns),
		}
		if err != nil {
			o.ErrMsg = err.Error()
		}
		if pe != nil {
			o.PredTask = pe.Name
		}
		return o, nil
	}
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("handler should return normally after catching the abort (process alive, panic recovered), got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[out](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if !o.HadErr {
		t.Fatal("Dag must return a non-nil error when a runIf predicate panics")
	}
	if !o.IsPredErr {
		t.Fatalf("Dag error should be a *DagPredicateError, got %q", o.ErrMsg)
	}
	if o.PredTask != "boom" {
		t.Fatalf("predicate error should name the offending task, got %q", o.PredTask)
	}
	if !o.ResNil {
		t.Fatal("Dag must not return a DagResult on a predicate abort")
	}
	if o.RefundRuns != 0 {
		t.Fatalf("downstream ALL_FAILED task must not run on a predicate abort, ran %d time(s)", o.RefundRuns)
	}
	if !strings.Contains(o.ErrMsg, "runIf") || !strings.Contains(o.ErrMsg, "panicked") {
		t.Fatalf("abort error should describe the runIf panic, got %q", o.ErrMsg)
	}
}
