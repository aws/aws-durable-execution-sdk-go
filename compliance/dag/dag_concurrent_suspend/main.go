// Command dag_concurrent_suspend is a deploy-only DAG conformance handler
// exercising inverted readiness across a SUSPEND boundary — the replay-flip
// case no other scenario covers. Two wait tasks of different durations both
// start in the first invocation, so the invocation suspends with TWO tasks
// in flight and resumes twice; the downstream pair then becomes ready in the
// reverse of registration order, one invocation apart. A counter-based id
// regression cannot reproduce the ids minted on the first invocation and
// fails replay-consistency; name-based ids are stable across the resumes.
//
// maxConcurrency is UNSET. Graph (suspenddag):
//
//	root(=1) -> slow(wait 8s) -.after-> afterSlow(="S") ┐
//	         -> fast(wait 2s) -.after-> afterFast(="F") ┴-> merge(="SF")
//
// Register slow before fast and afterSlow before afterFast, so afterFast
// becomes ready first. Timers (8s vs 2s), not races, decide the resume
// order, so the outcome is deterministic; the ~6s gap must not be narrowed.
// No peak-concurrency assertion is possible or needed here (the waits are
// not user code); the suspend boundary itself is the point.
//
// Returned summary: {reason, statuses{6}, counts[6,0,0,6], merge:"SF"}.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "suspenddag", func(d *durable.DagBuilder) {
		root := durable.DagStep(d, "root", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
		// Register slow (8s) BEFORE fast (2s): both waits start in the first
		// invocation, so the invocation suspends with two tasks in flight.
		slow := durable.DagWait(d, "slow", []durable.AnyHandle{root}, 8*time.Second)
		fast := durable.DagWait(d, "fast", []durable.AnyHandle{root}, 2*time.Second)
		// Register afterSlow BEFORE afterFast, but fast resolves first, so
		// afterFast becomes ready one invocation earlier — readiness inverts
		// registration order across the resumes.
		afterSlow := durable.DagStep(d, "afterSlow", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "S", nil }).
			After(slow)
		afterFast := durable.DagStep(d, "afterFast", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "F", nil }).
			After(fast)
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
	return sum, nil
}

func main() { durable.Start(handler) }
