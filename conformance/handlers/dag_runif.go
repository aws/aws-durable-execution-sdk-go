// DAG cloud handler "dag-runif": runIf value-branching. A classify step
// yields a value ("review"); three downstream branches each guard on
// WithRunIf against that value, so EXACTLY ONE branch (review) runs and
// the other two (publish, block) are SKIPPED with SkipReason
// RUN_IF_PREDICATE. Proves conditional per-task execution driven by an
// upstream task's typed result on a real deployed durable Lambda.
// Expected: classify + review SUCCEEDED, publish + block SKIPPED,
// CompletionReason ALL_COMPLETED (a runIf skip is not a failure), and the
// summary's Branch == "review".
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("dag-runif", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(dagRunIfHandler, config(client))
	})
}

func dagRunIfHandler(_ any, dc types.DurableContext) (*DagSummary, error) {
	res, err := dag.Dag(dc, "branch", func(d *dag.Context) {
		classify := dag.Step(d, "classify", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "review", nil
		})
		mk := func(name, want, out string) {
			dag.Step(d, name, []dag.AnyHandle{classify}, func(dp dag.Deps, _ dag.StepContext) (string, error) {
				return out, nil
			}, dag.WithRunIf(func(dp dag.Deps) bool {
				v, _ := dag.Get(dp, classify)
				return v == want
			}))
		}
		mk("publish", "publish", "published")
		mk("review", "review", "reviewed")
		mk("block", "block", "blocked")
	})
	if err != nil {
		return nil, err
	}
	statuses, counts := dagStatuses(res)
	branch := ""
	for name, st := range statuses {
		if name != "classify" && st == string(dag.StatusSucceeded) {
			branch = name
		}
	}
	return &DagSummary{
		Reason:   string(res.CompletionReason()),
		Statuses: statuses,
		Counts:   counts,
		Branch:   branch,
	}, nil
}
