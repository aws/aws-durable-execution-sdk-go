// DAG cloud handler "dag-compensation": trigger-rule-driven compensation.
//
//	                charge (FAILS)
//	          /          |          \
//	  fulfill        refund          audit
//	 ALL_SUCCESS    ALL_FAILED      ALL_DONE
//
// With charge failing: fulfill (ALL_SUCCESS) SKIPS, refund (ALL_FAILED)
// RUNS, audit (ALL_DONE) RUNS. The DAG drains fully with one failed task,
// so CompletionReason is COMPLETED_WITH_FAILURES and the durable
// execution itself still SUCCEEDS (task failures are reported inside the
// DagResult, not as the handler's error - see dag.Dag's own contract).
// This is the canonical saga/compensation shape: a failed primary action
// triggers its compensating action while the success-only follow-up is
// skipped and an always-run audit records the outcome.
package handlers

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("dag-compensation", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(dagCompensationHandler, config(client))
	})
}

func dagCompensationHandler(_ any, dc types.DurableContext) (*DagSummary, error) {
	res, err := dag.Dag(dc, "order", func(d *dag.Context) {
		charge := dag.Step(d, "charge", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "", errors.New("charge failed")
		})
		dag.Step(d, "fulfill", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "fulfilled", nil
		}).After(charge) // default ALL_SUCCESS -> skipped when charge fails
		dag.Step(d, "refund", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "refunded", nil
		}).After(charge).WithTrigger(dag.AllFailed) // compensating action
		dag.Step(d, "audit", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "audited", nil
		}).After(charge).WithTrigger(dag.AllDone) // always runs
	})
	if err != nil {
		return nil, err
	}
	statuses, counts := dagStatuses(res)
	return &DagSummary{
		Reason:   string(res.CompletionReason()),
		Statuses: statuses,
		Counts:   counts,
	}, nil
}
