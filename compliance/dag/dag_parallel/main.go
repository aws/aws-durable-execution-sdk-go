// Command dag_parallel is a deploy-only DAG conformance handler exercising a
// parallel task of two named branches feeding a downstream join step. It
// returns a dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: fork(DagParallel left->"L", right->"R") -> join(step[fork]).
//
// The join reads ONLY the aggregate ParallelResult/BatchResult handed to it
// as the dep value (success count and total branch count) and returns
// "<succeeded>/<size>" = "2/2". It does not read individual branch values,
// so this scenario is expressible identically across all four SDKs
// (Java cannot read parallel branch values from within a step body). Reading
// child branch values is covered separately by 10-6 (map).
//
// Expected: all SUCCEEDED, ALL_COMPLETED, join="2/2", counts [2,0,0,2].
package main

import (
	"fmt"

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
				// Aggregate-only join: success count over total branch count.
				return fmt.Sprintf("%d/%d", br.SuccessCount(), br.TotalCount()), nil
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
