// Command dag_nested is a deploy-only DAG conformance handler exercising a
// nested sub-DAG task between an upstream and a downstream step. It returns a
// dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: pre(=1) -> sub(SubDag[pre]: n1=2, n2[n1]=n1+3=5) -> post(n2*10)=50.
// Expected: all SUCCEEDED, ALL_COMPLETED, post=50, counts [3,0,0,3].
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "outerdag", func(d *durable.DagBuilder) {
		pre := durable.DagStep(d, "pre", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
		sub := durable.SubDag(d, "sub", []durable.AnyHandle{pre}, func(s *durable.DagBuilder) {
			n1 := durable.DagStep(s, "n1", nil,
				func(_ durable.Deps, _ durable.StepContext) (int, error) { return 2, nil })
			durable.DagStep(s, "n2", []durable.AnyHandle{n1},
				func(deps durable.Deps, _ durable.StepContext) (int, error) {
					v, e := durable.Get(deps, n1)
					return v + 3, e
				})
		}, durable.WithDagMaxConcurrency(1))
		durable.DagStep(d, "post", []durable.AnyHandle{sub},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				subRes, err := durable.Get(deps, sub)
				if err != nil {
					return 0, err
				}
				n2, err := durable.ResultByName[int](subRes, "n2")
				if err != nil {
					return 0, err
				}
				return n2 * 10, nil
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Post, _ = durable.ResultByName[int](res, "post")
	return sum, nil
}

func main() { durable.Start(handler) }
