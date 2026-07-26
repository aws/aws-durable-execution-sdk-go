package durable_test

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// wireID replicates the SDK's internal id hashing (durable.hashID): the
// first 16 hex chars of the MD5 digest of a positional operation id. The
// hashID function is unexported and this is an external (durable_test)
// package, so the scheme is reproduced here. It lets these tests assert the
// name-based task-id scheme documented in dag.go — a task's underlying op id
// is "{scopeId}-DAG_NODE_T_{name}", hashed to this wire form — WITHOUT
// reaching into unexported internals. If the SDK regressed to counter-based
// ids the recomputed name-based hash would not match any recorded op.
func wireID(positionalID string) string {
	sum := md5.Sum([]byte(positionalID))
	return hex.EncodeToString(sum[:])[:16]
}

// dagNodeIDPrefix mirrors the reserved id segment documented in dag.go: a
// DAG task's underlying op id is "{scopeId}-DAG_NODE_T_{name}". It is an
// unexported SDK constant, reproduced here for the external test package.
const dagNodeIDPrefix = "DAG_NODE_T_"

// TestDagE2E_ConcurrentOverlap is the local-runner guard for conformance
// scenario 10-13. It runs the overlapdag graph — root -> {slow(~2s),
// fast(~200ms)} -> {afterSlow,afterFast} -> merge, with registration order
// deliberately inverted against start/completion order — and asserts three
// order-invariant facts:
//
//  1. the result is "SsFf" and all six tasks succeeded;
//  2. peak OBSERVED concurrency was >= 2, i.e. the overlap genuinely
//     happened rather than the scheduler serializing the tasks;
//  3. every task's recorded operation id is the NAME-BASED id
//     "{scope}-DAG_NODE_T_{name}" — the assertion the cloud suite
//     deliberately cannot make because it only sees order-invariant history.
//
// A counter-based id regression would fail (3) here, and would also fail the
// EXECUTION on the wire (out-of-order completion -> replay-consistency
// error), which is the property that lets the cloud scenario assert outcome
// only.
func TestDagE2E_ConcurrentOverlap(t *testing.T) {
	type out struct {
		Merge   string
		Peak    int64
		Success int
		Reason  string
	}

	var cur, peak int64
	enter := func() {
		n := atomic.AddInt64(&cur, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if n <= p || atomic.CompareAndSwapInt64(&peak, p, n) {
				break
			}
		}
	}
	exit := func() { atomic.AddInt64(&cur, -1) }

	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "overlapdag", func(d *durable.DagBuilder) {
			root := durable.DagStep(d, "root", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				return 1, nil
			})
			slow := durable.DagStep(d, "slow", []durable.AnyHandle{root}, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				enter()
				defer exit()
				time.Sleep(2 * time.Second)
				return "S", nil
			})
			fast := durable.DagStep(d, "fast", []durable.AnyHandle{root}, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				enter()
				defer exit()
				time.Sleep(200 * time.Millisecond)
				return "F", nil
			})
			afterSlow := durable.DagStep(d, "afterSlow", []durable.AnyHandle{slow}, func(deps durable.Deps, _ durable.StepContext) (string, error) {
				v, _ := durable.Get(deps, slow)
				return v + "s", nil
			})
			afterFast := durable.DagStep(d, "afterFast", []durable.AnyHandle{fast}, func(deps durable.Deps, _ durable.StepContext) (string, error) {
				v, _ := durable.Get(deps, fast)
				return v + "f", nil
			})
			durable.DagStep(d, "merge", []durable.AnyHandle{afterSlow, afterFast}, func(deps durable.Deps, _ durable.StepContext) (string, error) {
				a, _ := durable.Get(deps, afterSlow)
				b, _ := durable.Get(deps, afterFast)
				return a + b, nil
			})
		})
		if err != nil {
			return out{}, err
		}
		if e := res.ThrowIfError(); e != nil {
			return out{}, e
		}
		merge, _ := durable.ResultByName[string](res, "merge")
		return out{Merge: merge, Peak: atomic.LoadInt64(&peak), Success: res.SucceededCount(), Reason: string(res.CompletionReason())}, nil
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

	// (1) order-invariant outcome.
	if o.Merge != "SsFf" {
		t.Fatalf("merge=%q want %q", o.Merge, "SsFf")
	}
	if o.Success != 6 || o.Reason != string(durable.AllCompleted) {
		t.Fatalf("unexpected aggregate: %+v", o)
	}

	// (2) the overlap genuinely occurred: two instrumented tasks (slow and
	// fast) were inside their bodies at the same time.
	if o.Peak < 2 {
		t.Fatalf("peak observed concurrency=%d, want >=2 (tasks did not overlap; scheduler serialized them)", o.Peak)
	}

	// (3) name-based ids. The DAG scope is the sole top-level op, so its
	// positional id is "1"; confirm that, then require each task op to carry
	// the name-based id "1-DAG_NODE_T_{name}".
	scope := dagScopeOp(t, result, "overlapdag")
	if want := wireID("1"); scope.ID != want {
		t.Fatalf("dag scope op id=%q, want hash of \"1\" (%q)", scope.ID, want)
	}
	for _, name := range []string{"root", "slow", "fast", "afterSlow", "afterFast", "merge"} {
		want := wireID("1-" + dagNodeIDPrefix + name)
		op := result.OperationByID(want)
		if op == nil {
			t.Fatalf("task %q: no op with name-based id %q (%s%s); a counter-based regression would fail here",
				name, want, dagNodeIDPrefix, name)
		}
		if op.Name != name {
			t.Fatalf("task %q: op with name-based id has Name=%q", name, op.Name)
		}
	}

	// Operation-count guard (mirrors the conformance runner's
	// ExpectedEventCount): the flat model checkpoints exactly N+1 ops — one
	// DAG scope CONTEXT plus one op per task — with no per-task wrapper.
	if got, want := len(result.Operations), 7; got != want {
		t.Fatalf("recorded %d ops, want %d (1 scope + 6 tasks); a wrapper-per-task regression would inflate this", got, want)
	}
}

// TestDagE2E_ConcurrentSuspend is the local-runner guard for conformance
// scenario 10-14: inverted readiness across a suspend. The suspenddag graph
// starts two wait tasks (8s and 2s) in the first invocation, so the
// invocation suspends with two tasks in flight; the downstream pair
// (registered afterSlow-before-afterFast) then becomes ready in the reverse
// of registration order. The assertion is order-invariant: the run resumes
// to "SF" with no replay-consistency error and all six tasks succeeded.
//
// The local runner advances all time-eligible waits together (virtual time,
// magnitude-independent), so the two waits resolve in one resume rather than
// two; the replay-flip property under name-based ids is what this guards,
// and a counter regression would surface as a FAILED execution here.
func TestDagE2E_ConcurrentSuspend(t *testing.T) {
	type out struct {
		Merge   string
		Success int
		Failure int
		Reason  string
	}
	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "suspenddag", func(d *durable.DagBuilder) {
			root := durable.DagStep(d, "root", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				return 1, nil
			})
			slow := durable.DagWait(d, "slow", []durable.AnyHandle{root}, 8*time.Second)
			fast := durable.DagWait(d, "fast", []durable.AnyHandle{root}, 2*time.Second)
			afterSlow := durable.DagStep(d, "afterSlow", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "S", nil
			}).After(slow)
			afterFast := durable.DagStep(d, "afterFast", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "F", nil
			}).After(fast)
			durable.DagStep(d, "merge", []durable.AnyHandle{afterSlow, afterFast}, func(deps durable.Deps, _ durable.StepContext) (string, error) {
				a, _ := durable.Get(deps, afterSlow)
				b, _ := durable.Get(deps, afterFast)
				return a + b, nil
			})
		})
		if err != nil {
			return out{}, err
		}
		if e := res.ThrowIfError(); e != nil {
			return out{}, e
		}
		merge, _ := durable.ResultByName[string](res, "merge")
		return out{Merge: merge, Success: res.SucceededCount(), Failure: res.FailureCount(), Reason: string(res.CompletionReason())}, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		// A counter-based id regression manifests as a replay-consistency
		// failure of the whole execution across the resume — exactly here.
		t.Fatalf("expected SUCCEEDED across the suspend/resume (a replay-consistency error would show here), got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[out](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if o.Merge != "SF" {
		t.Fatalf("merge=%q want %q", o.Merge, "SF")
	}
	if o.Success != 6 || o.Failure != 0 || o.Reason != string(durable.AllCompleted) {
		t.Fatalf("unexpected aggregate: %+v", o)
	}
}

// TestDagE2E_RunIfAbort is the local-runner guard for conformance scenario
// 10-12 and the runIf abort contract (RUNIF_ABORT_CONTRACT / H5). The
// abortdag graph is gate(=1) -> guarded[runIf throws] -.after-> refund
// (ALL_FAILED). A throwing predicate is a defect in deterministic code, so
// the DAG ABORTS with a typed *DagPredicateError: guarded's body never runs,
// and refund's ALL_FAILED compensation must NOT fire off a scheduler-side
// defect. The recovered panic's cause ("predicate boom") is reachable via
// errors.Is/As.
func TestDagE2E_RunIfAbort(t *testing.T) {
	var gateRuns, guardedBodyRuns, refundRuns int32
	predBoom := errors.New("predicate boom")

	type out struct {
		HadErr       bool
		IsPredErr    bool
		PredTask     string
		CauseReached bool
		ResNil       bool
		GateRuns     int32
		GuardedRuns  int32
		RefundRuns   int32
	}
	handler := func(dc durable.Context, _ struct{}) (out, error) {
		res, err := durable.Dag(dc, "abortdag", func(d *durable.DagBuilder) {
			gate := durable.DagStep(d, "gate", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				atomic.AddInt32(&gateRuns, 1)
				return 1, nil
			})
			guarded := durable.DagStep(d, "guarded", []durable.AnyHandle{gate},
				func(_ durable.Deps, _ durable.StepContext) (string, error) {
					atomic.AddInt32(&guardedBodyRuns, 1)
					return "ran", nil
				},
				durable.WithRunIf(func(durable.Deps) bool { panic(predBoom) }))
			durable.DagStep(d, "refund", nil,
				func(_ durable.Deps, _ durable.StepContext) (string, error) {
					atomic.AddInt32(&refundRuns, 1)
					return "refunded", nil
				}).After(guarded).WithTrigger(durable.AllFailed)
		}, durable.WithDagMaxConcurrency(1))

		var pe *durable.DagPredicateError
		o := out{
			HadErr:       err != nil,
			IsPredErr:    errors.As(err, &pe),
			CauseReached: errors.Is(err, predBoom),
			ResNil:       res == nil,
			GateRuns:     atomic.LoadInt32(&gateRuns),
			GuardedRuns:  atomic.LoadInt32(&guardedBodyRuns),
			RefundRuns:   atomic.LoadInt32(&refundRuns),
		}
		if pe != nil {
			o.PredTask = pe.Name
		}
		return o, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, struct{}{})
	if result.Status != durabletest.Succeeded {
		t.Fatalf("handler should return normally after catching the abort, got %s (%+v)", result.Status, result.Error)
	}
	o, err := durabletest.ResultAs[out](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if !o.HadErr || !o.IsPredErr {
		t.Fatalf("Dag must return a *DagPredicateError on a throwing runIf: %+v", o)
	}
	if o.PredTask != "guarded" {
		t.Fatalf("predicate error should name the offending task, got %q", o.PredTask)
	}
	if !o.CauseReached {
		t.Fatal("original predicate error should be reachable via errors.Is (wrapped cause)")
	}
	if !o.ResNil {
		t.Fatal("Dag must not return a DagResult on a predicate abort")
	}
	if o.GateRuns != 1 {
		t.Fatalf("gate should run exactly once, ran %d", o.GateRuns)
	}
	if o.GuardedRuns != 0 {
		t.Fatalf("guarded body must never run (only its predicate threw), ran %d", o.GuardedRuns)
	}
	if o.RefundRuns != 0 {
		t.Fatalf("ALL_FAILED compensation must NOT run on a predicate abort, ran %d", o.RefundRuns)
	}
}

// dagScopeOp returns the single CONTEXT/SubType=Dag scope operation with the
// given DAG name, failing the test if it is absent.
func dagScopeOp(t *testing.T, r *durabletest.TestResult, dagName string) *durabletest.TestOperation {
	t.Helper()
	for i := range r.Operations {
		op := &r.Operations[i]
		if op.Type == "CONTEXT" && op.SubType == "Dag" && op.Name == dagName {
			return op
		}
	}
	t.Fatalf("no DAG scope op (CONTEXT/Dag) named %q found among %d ops", dagName, len(r.Operations))
	return nil
}
