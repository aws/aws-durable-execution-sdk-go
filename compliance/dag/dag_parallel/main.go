// Command dag_parallel is a deploy-only DAG conformance handler exercising a
// parallel task of two named branches feeding a downstream join step. It
// returns a dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: fork(DagParallel left->"L", right->"R") -> join(step[fork])="L-R".
// Expected: all SUCCEEDED, ALL_COMPLETED, join="L-R", counts [2,0,0,2].
package main

import (
	"strings"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "paralleldag", func(d *durable.DagBuilder) {
		branches := []durable.Branch[string]{
			{Name: "left", Func: func(cc durable.Context) (string, error) {
				return durable.Step(cc, "left", func(_ durable.StepContext) (string, error) { return "L", nil })
			}},
			{Name: "right", Func: func(cc durable.Context) (string, error) {
				return durable.Step(cc, "right", func(_ durable.StepContext) (string, error) { return "R", nil })
			}},
		}
		fork := durable.DagParallel(d, "fork", nil, branches, durable.WithBatchMaxConcurrency(1))
		durable.DagStep(d, "join", []durable.AnyHandle{fork},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				br, err := durable.Get(deps, fork)
				if err != nil {
					return "", err
				}
				return strings.Join(br.Results(), "-"), nil
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Join, _ = durable.ResultByName[string](res, "join")
	return sum, nil
}

func main() { durable.Start(handler) }
