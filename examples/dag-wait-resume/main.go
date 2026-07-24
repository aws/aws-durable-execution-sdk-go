// Command dag-wait-resume demonstrates an in-DAG [durable.DagWait] task that
// suspends the whole invocation between two steps. When the wait elapses the
// invocation resumes in a fresh Lambda invocation; the DAG re-executes and
// each already-settled task hits its per-operation checkpoint fast-path, so
// the "finish" step's marker proves the workflow ran to completion across the
// suspend/resume boundary.
//
// Graph: start -> pause(Wait 5s) -> finish(="resumed").
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// waitResumeResult is the JSON-serializable projection of the DagResult.
type waitResumeResult struct {
	Marker    string `json:"marker"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Skipped   int    `json:"skipped"`
	Total     int    `json:"total"`
	Reason    string `json:"reason"`
}

func handler(ctx durable.Context, _ any) (waitResumeResult, error) {
	res, err := durable.Dag(ctx, "waitresume", func(d *durable.DagBuilder) {
		start := durable.DagStep(d, "start", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "started", nil })
		pause := durable.DagWait(d, "pause", []durable.AnyHandle{start}, 5*time.Second)
		durable.DagStep(d, "finish", []durable.AnyHandle{pause},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "resumed", nil })
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return waitResumeResult{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return waitResumeResult{}, err
	}
	marker, err := durable.ResultByName[string](res, "finish")
	if err != nil {
		return waitResumeResult{}, err
	}
	return waitResumeResult{
		Marker:    marker,
		Succeeded: res.SucceededCount(),
		Failed:    res.FailureCount(),
		Skipped:   res.SkippedCount(),
		Total:     res.TotalCount(),
		Reason:    string(res.CompletionReason()),
	}, nil
}

func main() { durable.Start(handler) }
