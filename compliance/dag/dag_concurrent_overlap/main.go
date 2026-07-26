// Command dag_concurrent_overlap is a deploy-only DAG conformance handler
// that forces two tasks to run at the same time INSIDE one invocation, so a
// regression from name-based task ids to a plain counter fails the execution
// with a replay-consistency error instead of passing silently. Every other
// DAG scenario pins maxConcurrency to 1, at which a counter and a name-based
// id emit identical histories; only genuine overlap with out-of-order
// completion distinguishes them.
//
// maxConcurrency is UNSET (unbounded). Graph (overlapdag):
//
//	root(=1) -> slow(sleep ~2s, ="S")   -> afterSlow(=slow+"s"="Ss") ┐
//	         -> fast(sleep ~200ms, ="F") -> afterFast(=fast+"f"="Ff") ┴-> merge(="SsFf")
//
// Registration order is deliberately inverted against start order: slow is
// registered before fast, and afterSlow before afterFast, so afterFast
// becomes ready (and starts) FIRST. That inversion is the condition a
// counter cannot survive.
//
// A shared atomic gauge counts entries to slow/fast and tracks the peak, so
// the scenario is not silently vacuous if a future change serializes the
// scheduler. Because both bodies are plain in-body sleeps (not wait ops),
// the whole DAG runs in a single invocation and the peak is observed live.
//
// Returned summary: {reason, statuses{6}, counts[6,0,0,6], merge:"SsFf",
// peakConcurrency:2}. This asserts ONLY order-invariant facts.
package main

import (
	"sync/atomic"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	// cur is the number of instrumented tasks currently running; peak is the
	// maximum ever observed. Tasks run on their own goroutines, so both are
	// touched only via sync/atomic.
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

	res, err := durable.Dag(ctx, "overlapdag", func(d *durable.DagBuilder) {
		root := durable.DagStep(d, "root", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
		// Register slow BEFORE fast; slow's longer sleep guarantees fast
		// finishes first, so completion order inverts start order.
		slow := durable.DagStep(d, "slow", []durable.AnyHandle{root},
			func(_ durable.Deps, _ durable.StepContext) (string, error) {
				enter()
				defer exit()
				time.Sleep(2 * time.Second)
				return "S", nil
			})
		fast := durable.DagStep(d, "fast", []durable.AnyHandle{root},
			func(_ durable.Deps, _ durable.StepContext) (string, error) {
				enter()
				defer exit()
				time.Sleep(200 * time.Millisecond)
				return "F", nil
			})
		// Register afterSlow BEFORE afterFast; afterFast's dep (fast)
		// finishes first, so afterFast becomes ready and starts first —
		// earlier than afterSlow despite being registered later.
		afterSlow := durable.DagStep(d, "afterSlow", []durable.AnyHandle{slow},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				v, _ := durable.Get(deps, slow)
				return v + "s", nil
			})
		afterFast := durable.DagStep(d, "afterFast", []durable.AnyHandle{fast},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				v, _ := durable.Get(deps, fast)
				return v + "f", nil
			})
		durable.DagStep(d, "merge", []durable.AnyHandle{afterSlow, afterFast},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				a, _ := durable.Get(deps, afterSlow)
				b, _ := durable.Get(deps, afterFast)
				return a + b, nil
			})
	})
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Merge, _ = durable.ResultByName[string](res, "merge")
	sum.PeakConcurrency = int(atomic.LoadInt64(&peak))
	return sum, nil
}

func main() { durable.Start(handler) }
