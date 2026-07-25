// Command dag_invoke is a deploy-only DAG conformance handler exercising an
// invoke task inside a DAG, wired to the shared echo target function. It
// returns a dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: prep(step)=21 -> call(DagInvoke[prep], payload=prep) -> done(step)=call*2.
// The invoke target is the echo function named by TARGET_FUNCTION_NAME, which
// echoes its input, so call resolves to 21 and done to 42. The echo target
// waits briefly, so this scenario suspends and resumes across invocations.
//
// Expected: all SUCCEEDED, ALL_COMPLETED, call=21, done=42, counts [3,0,0,3].
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	target := os.Getenv("TARGET_FUNCTION_NAME")

	res, err := durable.Dag(ctx, "invokedag", func(d *durable.DagBuilder) {
		prep := durable.DagStep(d, "prep", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 21, nil })
		call := durable.DagInvoke[int, int](d, "call", target, []durable.AnyHandle{prep},
			func(deps durable.Deps) (int, error) {
				return durable.Get(deps, prep)
			})
		durable.DagStep(d, "done", []durable.AnyHandle{call},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, e := durable.Get(deps, call)
				return v * 2, e
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Call, _ = durable.ResultByName[int](res, "call")
	sum.Done, _ = durable.ResultByName[int](res, "done")
	return sum, nil
}

func main() { durable.Start(handler) }
