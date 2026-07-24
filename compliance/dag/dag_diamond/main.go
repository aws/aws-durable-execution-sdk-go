// Command dag_diamond is a deploy-only DAG conformance handler exercising a
// diamond fan-out/fan-in graph with typed dependency injection. It returns a
// dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: fetch(=10) -> {ta(+1)=11, tb(*2)=20} -> merge(ta+tb)=31.
// Expected: all SUCCEEDED, ALL_COMPLETED, merge=31, counts [4,0,0,4].
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "diamond", func(d *durable.DagBuilder) {
		fetch := durable.DagStep(d, "fetch", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 10, nil })
		ta := durable.DagStep(d, "ta", []durable.AnyHandle{fetch},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, e := durable.Get(deps, fetch)
				return v + 1, e
			})
		tb := durable.DagStep(d, "tb", []durable.AnyHandle{fetch},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, e := durable.Get(deps, fetch)
				return v * 2, e
			})
		durable.DagStep(d, "merge", []durable.AnyHandle{ta, tb},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				av, _ := durable.Get(deps, ta)
				bv, _ := durable.Get(deps, tb)
				return av + bv, nil
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Merge, _ = durable.ResultByName[int](res, "merge")
	return sum, nil
}

func main() { durable.Start(handler) }
