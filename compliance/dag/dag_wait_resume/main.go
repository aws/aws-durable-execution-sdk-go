// Command dag_wait_resume is a deploy-only DAG conformance handler exercising
// an in-graph Wait task that suspends the whole invocation between two steps,
// then resumes in a fresh invocation. The "finish" step marker proves the DAG
// ran to completion across the suspend/resume boundary. It returns a
// dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: start -> pause(Wait 5s) -> finish(="resumed"). Expected: all
// SUCCEEDED, ALL_COMPLETED, marker="resumed", counts [3,0,0,3].
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "waitresume", func(d *durable.DagBuilder) {
		start := durable.DagStep(d, "start", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "started", nil })
		pause := durable.DagWait(d, "pause", []durable.AnyHandle{start}, 5*time.Second)
		durable.DagStep(d, "finish", []durable.AnyHandle{pause},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "resumed", nil })
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Marker, _ = durable.ResultByName[string](res, "finish")
	return sum, nil
}

func main() { durable.Start(handler) }
