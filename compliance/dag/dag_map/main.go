// Command dag_map is a deploy-only DAG conformance handler exercising a map
// task over a fixed item list feeding a downstream aggregation step. It
// returns a dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: squares(DagMap [1,2] -> [1,4]) -> sum(step[squares])=5. Expected:
// all SUCCEEDED, ALL_COMPLETED, sum=5, counts [2,0,0,2].
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "mapdag", func(d *durable.DagBuilder) {
		squares := durable.DagMap(d, "squares", nil,
			func(_ durable.Deps) []int { return []int{1, 2} },
			func(cc durable.Context, item int, _ int) (int, error) {
				return durable.Step(cc, "square", func(_ durable.StepContext) (int, error) { return item * item, nil })
			},
			durable.WithBatchMaxConcurrency(1))
		durable.DagStep(d, "sum", []durable.AnyHandle{squares},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				br, err := durable.Get(deps, squares)
				if err != nil {
					return 0, err
				}
				total := 0
				for _, v := range br.Results() {
					total += v
				}
				return total, nil
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Sum, _ = durable.ResultByName[int](res, "sum")
	return sum, nil
}

func main() { durable.Start(handler) }
