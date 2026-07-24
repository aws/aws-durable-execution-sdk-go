// DAG cloud handler "dag-wait": a DAG containing a Wait task that
// suspends the execution, proving durable replay through a DAG.
//
//	start -> pause (Wait 5s) -> finish
//
// On a real deployed durable Lambda the Wait task suspends the whole
// invocation (dag.Dag returns operations.ErrSuspended, which
// WithDurableExecution turns into a real Lambda suspend); the backend
// re-invokes after the delay and the DAG replays - start's checkpointed
// result is restored WITHOUT re-executing, the Wait fast-path completes,
// and finish runs post-suspend. finish returning its Marker only after
// the resume is exactly what proves durable replay carried the DAG across
// the suspend boundary. Expected: all tasks SUCCEEDED, CompletionReason
// ALL_COMPLETED, Marker == "resumed".
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("dag-wait", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(dagWaitHandler, config(client))
	})
}

func dagWaitHandler(_ any, dc types.DurableContext) (*DagSummary, error) {
	res, err := dag.Dag(dc, "waitdag", func(d *dag.Context) {
		start := dag.Step(d, "start", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "started", nil
		})
		pause := dag.Wait(d, "pause", []dag.AnyHandle{start}, dag.Duration{Seconds: 5})
		dag.Step(d, "finish", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "resumed", nil
		}).After(pause)
	})
	if err != nil {
		return nil, err
	}
	statuses, counts := dagStatuses(res)
	marker, _ := dag.ResultByName[string](res, "finish")
	return &DagSummary{
		Reason:   string(res.CompletionReason()),
		Statuses: statuses,
		Counts:   counts,
		Marker:   marker,
	}, nil
}
