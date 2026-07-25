// Command dag_wait_for_condition is a deploy-only DAG conformance handler
// exercising a waitForCondition task that polls to a threshold, then feeds a
// downstream step across the suspend/resume boundary. It returns a
// dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: poll(DagWaitForCondition 0, +1/poll, stop@2)=2 -> done(poll*5)=10.
// Expected: all SUCCEEDED, ALL_COMPLETED, poll=2, done=10, counts [2,0,0,2].
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "wfcdag", func(d *durable.DagBuilder) {
		poll := durable.DagWaitForCondition(d, "poll", nil, 0,
			func(_ durable.Deps, state int, _ durable.StepContext) (int, error) { return state + 1, nil },
			durable.WithCondition(func(state int) bool { return state >= 2 }))
		durable.DagStep(d, "done", []durable.AnyHandle{poll},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				p, e := durable.Get(deps, poll)
				return p * 5, e
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Poll, _ = durable.ResultByName[int](res, "poll")
	sum.Done, _ = durable.ResultByName[int](res, "done")
	return sum, nil
}

func main() { durable.Start(handler) }
