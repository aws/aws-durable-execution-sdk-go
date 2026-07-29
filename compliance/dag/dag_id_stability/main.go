// Command dag_id_stability is a deploy-only DAG conformance handler for
// 10-20: task-id stability across independently forced completion orders.
//
// Identical shape to 10-13's overlap (dag_concurrent_overlap) -- root ->
// {a, b} -> {afterA, afterB} -> merge, maxConcurrency unbounded -- except
// which sibling sleeps longer is driven by the input's Swap flag: swap=false
// makes a finish first; swap=true makes b finish first. Both invocations
// register the SAME task names in the SAME order every time -- only the
// RUNTIME completion order changes.
//
// This is the harness-level counterpart to 10-13: 10-13 proves out-of-order
// completion doesn't fail the execution (an INDIRECT proof of name-based
// ids, since a counter-based scheme would trip the SDK's own
// replay-consistency check). This scenario is invoked TWICE by a dedicated
// script (id_stability.py, not the normal single-invocation validator) with
// swap flipped between runs, and asserts each task's Id field in the
// captured execution history is IDENTICAL across both runs -- the direct
// proof that ids are derived from the task name, not from completion order
// or a counter.
//
// Returned summary: {reason, statuses{6}, counts[6,0,0,6], merge:"AaBb"}.
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type idStabilityInput struct {
	Swap bool `json:"swap"`
}

func handler(ctx durable.Context, in idStabilityInput) (dagsummary.Summary, error) {
	swap := in.Swap

	res, err := durable.Dag(ctx, "idstabilitydag", func(d *durable.DagBuilder) {
		root := durable.DagStep(d, "root", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
		a := durable.DagStep(d, "a", []durable.AnyHandle{root},
			func(_ durable.Deps, _ durable.StepContext) (string, error) {
				if swap {
					time.Sleep(2 * time.Second)
				} else {
					time.Sleep(200 * time.Millisecond)
				}
				return "A", nil
			})
		b := durable.DagStep(d, "b", []durable.AnyHandle{root},
			func(_ durable.Deps, _ durable.StepContext) (string, error) {
				if swap {
					time.Sleep(200 * time.Millisecond)
				} else {
					time.Sleep(2 * time.Second)
				}
				return "B", nil
			})
		afterA := durable.DagStep(d, "afterA", []durable.AnyHandle{a},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				v, _ := durable.Get(deps, a)
				return v + "a", nil
			})
		afterB := durable.DagStep(d, "afterB", []durable.AnyHandle{b},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				v, _ := durable.Get(deps, b)
				return v + "b", nil
			})
		durable.DagStep(d, "merge", []durable.AnyHandle{afterA, afterB},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				x, _ := durable.Get(deps, afterA)
				y, _ := durable.Get(deps, afterB)
				return x + y, nil
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
