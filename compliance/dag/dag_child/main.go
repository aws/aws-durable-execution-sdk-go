// Command dag_child is a deploy-only DAG conformance handler exercising a
// runInChildContext task nested between an upstream and a downstream step.
// It returns a dagsummary.Summary as its top-level result for cloud
// assertion.
//
// Graph: seed(=1) -> group(DagChild[seed]: inner-a=2, inner-b=3, ret 5) ->
// done(group*2)=10. Expected: all SUCCEEDED, ALL_COMPLETED, group=5,
// done=10, counts [3,0,0,3].
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "childdag", func(d *durable.DagBuilder) {
		seed := durable.DagStep(d, "seed", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
		group := durable.DagChild(d, "group", []durable.AnyHandle{seed},
			func(deps durable.Deps, cc durable.Context) (int, error) {
				s, _ := durable.Get(deps, seed)
				a, err := durable.Step(cc, "inner-a",
					func(_ durable.StepContext) (int, error) { return s + 1, nil })
				if err != nil {
					return 0, err
				}
				b, err := durable.Step(cc, "inner-b",
					func(_ durable.StepContext) (int, error) { return s + 2, nil })
				if err != nil {
					return 0, err
				}
				return a + b, nil
			})
		durable.DagStep(d, "done", []durable.AnyHandle{group},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				g, e := durable.Get(deps, group)
				return g * 2, e
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Group, _ = durable.ResultByName[int](res, "group")
	sum.Done, _ = durable.ResultByName[int](res, "done")
	return sum, nil
}

func main() { durable.Start(handler) }
