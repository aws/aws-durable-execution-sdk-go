// Command dag-diamond demonstrates the EXPERIMENTAL [durable.Dag] DAG
// orchestration layer with a classic diamond graph: a single source fans
// out to two independent transforms that then fan back in to a merge task.
// It exercises typed dependency injection via [durable.Get] across tasks.
//
// Graph: fetch(=10) -> {ta(+1)=11, tb(*2)=20} -> merge(ta+tb)=31.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

// diamondResult is the JSON-serializable projection of the DagResult.
type diamondResult struct {
	Merge     int    `json:"merge"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Skipped   int    `json:"skipped"`
	Total     int    `json:"total"`
	Reason    string `json:"reason"`
}

func handler(ctx durable.Context, _ any) (diamondResult, error) {
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
		return diamondResult{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return diamondResult{}, err
	}
	merge, err := durable.ResultByName[int](res, "merge")
	if err != nil {
		return diamondResult{}, err
	}
	return diamondResult{
		Merge:     merge,
		Succeeded: res.SucceededCount(),
		Failed:    res.FailureCount(),
		Skipped:   res.SkippedCount(),
		Total:     res.TotalCount(),
		Reason:    string(res.CompletionReason()),
	}, nil
}

func main() { durable.Start(handler) }
