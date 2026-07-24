// DAG cloud handler "dag-diamond": a diamond fan-out/fan-in with typed
// inter-task dependencies resolved via dag.Get[T].
//
//	        fetch (=10)
//	        /      \
//	     ta(+1)   tb(*2)      -> 11, 20
//	        \      /
//	         merge(a+b)       -> 31
//
// Proves the DAG primitive schedules a fan-out/fan-in graph end-to-end on
// a real deployed durable Lambda and that each downstream task reads its
// upstreams' typed results through Deps (dag.Get). Expected outcome:
// every task SUCCEEDED, CompletionReason ALL_COMPLETED, merge == 31.
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/checkpoint"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func init() {
	Register("dag-diamond", func(client checkpoint.Client) Handler {
		return durable.WithDurableExecution(dagDiamondHandler, config(client))
	})
}

func dagDiamondHandler(_ any, dc types.DurableContext) (*DagSummary, error) {
	res, err := dag.Dag(dc, "diamond", func(d *dag.Context) {
		fetch := dag.Step(d, "fetch", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) {
			return 10, nil
		})
		ta := dag.Step(d, "ta", []dag.AnyHandle{fetch}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			v, _ := dag.Get(dp, fetch)
			return v + 1, nil
		})
		tb := dag.Step(d, "tb", []dag.AnyHandle{fetch}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			v, _ := dag.Get(dp, fetch)
			return v * 2, nil
		})
		dag.Step(d, "merge", []dag.AnyHandle{ta, tb}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			a, _ := dag.Get(dp, ta)
			b, _ := dag.Get(dp, tb)
			return a + b, nil
		})
	})
	if err != nil {
		return nil, err
	}
	statuses, counts := dagStatuses(res)
	merge, _ := dag.ResultByName[int](res, "merge")
	return &DagSummary{
		Reason:   string(res.CompletionReason()),
		Statuses: statuses,
		Counts:   counts,
		Merge:    merge,
	}, nil
}
