// Command dag_run_if is a deploy-only DAG conformance handler exercising
// per-task conditional execution via WithRunIf: a classifier produces a
// label and three branches each run only if the label matches their own
// name. It returns a dagsummary.Summary as its top-level result for cloud
// assertion.
//
// Graph: classify(="review") -> {publish, review, block}, each guarded by
// WithRunIf(Get(classify) == branch). Expected: classify+review SUCCEEDED,
// publish+block SKIPPED (RUN_IF_PREDICATE), ALL_COMPLETED, branch="review",
// counts [2,0,2,4].
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	const label = "review"
	branches := []string{"publish", "review", "block"}

	res, err := durable.Dag(ctx, "runif", func(d *durable.DagBuilder) {
		classify := durable.DagStep(d, "classify", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return label, nil })
		for _, name := range branches {
			name := name
			durable.DagStep(d, name, []durable.AnyHandle{classify},
				func(_ durable.Deps, _ durable.StepContext) (string, error) { return name, nil },
				durable.WithRunIf(func(deps durable.Deps) bool {
					v, _ := durable.Get(deps, classify)
					return v == name
				}))
		}
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	for _, name := range branches {
		if sum.Statuses[name] == string(durable.StatusSucceeded) {
			sum.Branch = name
			break
		}
	}
	return sum, nil
}

func main() { durable.Start(handler) }
