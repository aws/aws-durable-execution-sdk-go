package dag_test

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// chunkBytes is the per-task result size. Six tasks * 64KB = 384KB > the
// 256KB checkpoint limit, so the aggregate DagResult is oversize and rides
// the base SDK's ReplayChildren offload (DAG_SPEC_GO.md §12, rows 13/14),
// while each individual step result stays well under the single-op limit.
const chunkBytes = 64 * 1024

const bigTaskCount = 6

// TestDag_LargePayloadReplay covers DAG_SPEC_GO.md §12 "large-payload
// reconstruction via ReplayChildren re-execution": it builds a DAG whose
// aggregate DagResult exceeds 256KB, drives a suspend/resume via the
// SkipTime local runner, and asserts (a) every large per-task result is
// reconstructed intact across the resume and (b) completed task bodies are
// not re-executed on replay (their per-op checkpoint fast-path is hit).
func TestDag_LargePayloadReplay(t *testing.T) {
	var runs [bigTaskCount]int32

	type out struct {
		TotalLen int
		Success  int
		Reason   string
	}

	handler := func(_ struct{}, dc types.DurableContext) (out, error) {
		res, err := dag.Dag(dc, "bigpayload", func(d *dag.Context) {
			seed := dag.Step(d, "seed", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) {
				return chunkBytes, nil
			})
			bigs := make([]dag.AnyHandle, 0, bigTaskCount)
			for i := 0; i < bigTaskCount; i++ {
				i := i
				h := dag.Step(d, bigName(i), []dag.AnyHandle{seed}, func(deps dag.Deps, _ dag.StepContext) (string, error) {
					atomic.AddInt32(&runs[i], 1)
					n, _ := dag.Get(deps, seed)
					return strings.Repeat("x", n), nil
				})
				bigs = append(bigs, h)
			}
			// A wait gated behind the big tasks forces a suspend AFTER their
			// (large) results are checkpointed, so the resume must reconstruct
			// them rather than re-run the bodies.
			dag.Wait(d, "pause", bigs, types.Duration{Seconds: 5})
		})
		if err != nil {
			return out{}, err
		}
		if e := res.Err(); e != nil {
			return out{}, e
		}
		total := 0
		for i := 0; i < bigTaskCount; i++ {
			v, rerr := dag.ResultByName[string](res, bigName(i))
			if rerr != nil {
				return out{}, rerr
			}
			total += len(v)
		}
		return out{TotalLen: total, Success: res.SuccessCount(), Reason: res.CompletionReason()}, nil
	}

	runner := dtesting.New(handler, nil) // SkipTime defaults on
	result, err := runner.Run(struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	o, err := dtesting.GetResult[out](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// Aggregate DagResult reconstructed intact across the suspend/resume.
	wantLen := bigTaskCount * chunkBytes
	if o.TotalLen != wantLen {
		t.Fatalf("reconstructed payload length=%d want %d", o.TotalLen, wantLen)
	}
	if want := bigTaskCount + 2; o.Success != want { // seed + big tasks + pause wait
		t.Fatalf("success count=%d want %d", o.Success, want)
	}
	if o.Reason != dag.AllCompleted {
		t.Fatalf("reason=%q want AllCompleted", o.Reason)
	}
	// Each big-task body ran exactly once despite the suspend/resume replay.
	for i := 0; i < bigTaskCount; i++ {
		if got := atomic.LoadInt32(&runs[i]); got != 1 {
			t.Fatalf("task %s body ran %d times, want 1 (checkpoint fast-path not hit on replay)", bigName(i), got)
		}
	}
}

// TestDag_OrderIndependenceReplay covers DAG_SPEC_GO.md §12
// "order-independence": it runs the same independent-branch (diamond) DAG
// twice, forcing the two parallel branches to settle in opposite orders, and
// asserts the resulting DagResult is identical with no non-determinism /
// replay-consistency error.
func TestDag_OrderIndependenceReplay(t *testing.T) {
	type out struct {
		Merged  int
		Success int
		Failure int
		Reason  string
	}

	// runDiamond runs fetch -> {a,b} -> merge. gate sequences the two
	// branches: if aFirst, branch b blocks until a signals; otherwise a
	// blocks until b signals. A sync.Once guards the close so a replayed
	// body (should one occur) cannot double-close.
	runDiamond := func(aFirst bool) (out, error) {
		signal := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(signal) }) }

		handler := func(_ struct{}, dc types.DurableContext) (out, error) {
			res, derr := dag.Dag(dc, "diamond", func(d *dag.Context) {
				fetch := dag.Step(d, "fetch", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) {
					return 10, nil
				})
				a := dag.Step(d, "a", []dag.AnyHandle{fetch}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
					if aFirst {
						release()
					} else {
						<-signal
					}
					v, _ := dag.Get(deps, fetch)
					return v + 1, nil
				})
				b := dag.Step(d, "b", []dag.AnyHandle{fetch}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
					if aFirst {
						<-signal
					} else {
						release()
					}
					v, _ := dag.Get(deps, fetch)
					return v + 2, nil
				})
				dag.Step(d, "merge", []dag.AnyHandle{a, b}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
					av, _ := dag.Get(deps, a)
					bv, _ := dag.Get(deps, b)
					return av + bv, nil
				})
			})
			if derr != nil {
				return out{}, derr
			}
			if e := res.Err(); e != nil {
				return out{}, e
			}
			merge, _ := dag.ResultByName[int](res, "merge")
			return out{Merged: merge, Success: res.SuccessCount(), Failure: res.FailureCount(), Reason: res.CompletionReason()}, nil
		}

		runner := dtesting.New(handler, nil)
		result, rerr := runner.Run(struct{}{})
		if rerr != nil {
			return out{}, rerr
		}
		if result.GetStatus() != types.ExecutionStatusSucceeded {
			msg, _ := result.GetError()
			t.Fatalf("expected SUCCEEDED (aFirst=%v), got %s (%s)", aFirst, result.GetStatus(), msg)
		}
		return dtesting.GetResult[out](result)
	}

	aThenB, err := runDiamond(true)
	if err != nil {
		t.Fatalf("run aFirst: %v", err)
	}
	bThenA, err := runDiamond(false)
	if err != nil {
		t.Fatalf("run bFirst: %v", err)
	}

	// Identical aggregate DagResult regardless of branch settle order.
	if aThenB != bThenA {
		t.Fatalf("order-dependent result: aFirst=%+v bFirst=%+v", aThenB, bThenA)
	}
	// a=11, b=12 => merge=23; 4 successes, no failures.
	if aThenB.Merged != 23 || aThenB.Success != 4 || aThenB.Failure != 0 || aThenB.Reason != dag.AllCompleted {
		t.Fatalf("unexpected diamond result: %+v", aThenB)
	}
}

func bigName(i int) string {
	return "big_" + string(rune('0'+i))
}
