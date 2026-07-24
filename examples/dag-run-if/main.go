// Command dag-run-if demonstrates the EXPERIMENTAL [durable.Dag] per-task
// conditional execution via [durable.WithRunIf]: a classifier task produces a
// label, and three branch tasks each run only if the label matches their own
// name. Non-matching branches are skipped with reason RUN_IF_PREDICATE.
//
// Graph: classify(="review") -> {publish, review, block}, each guarded by
// WithRunIf(Get(classify) == <branch name>). Only "review" runs.
package main

import "github.com/aws/aws-durable-execution-sdk-go/durable"

// runIfResult is the JSON-serializable projection of the DagResult.
type runIfResult struct {
	Branch    string            `json:"branch"` // the branch that actually ran
	Statuses  map[string]string `json:"statuses"`
	Succeeded int               `json:"succeeded"`
	Failed    int               `json:"failed"`
	Skipped   int               `json:"skipped"`
	Total     int               `json:"total"`
	Reason    string            `json:"reason"`
}

func handler(ctx durable.Context, _ any) (runIfResult, error) {
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
		return runIfResult{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return runIfResult{}, err
	}

	statuses := make(map[string]string, len(res.Results()))
	for name, te := range res.Results() {
		statuses[name] = string(te.Status)
	}
	ran := ""
	for _, name := range branches {
		if statuses[name] == string(durable.StatusSucceeded) {
			ran = name
			break
		}
	}
	return runIfResult{
		Branch:    ran,
		Statuses:  statuses,
		Succeeded: res.SucceededCount(),
		Failed:    res.FailureCount(),
		Skipped:   res.SkippedCount(),
		Total:     res.TotalCount(),
		Reason:    string(res.CompletionReason()),
	}, nil
}

func main() { durable.Start(handler) }
