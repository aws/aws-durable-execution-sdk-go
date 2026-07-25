// Command dag_callback is a deploy-only DAG conformance handler exercising a
// callback (wait-for-callback) task inside a DAG. It returns a
// dagsummary.Summary as its top-level result for cloud assertion.
//
// Graph: pre(step)="ready" -> cb(DagCallback[pre], no-op submitter) ->
// post(step)=cb+"_done".
//
// The submitter receives the generated callback id and does nothing durable
// (same as the wait_for_callback 7-1 handler). The conformance runner
// completes the callback externally with a success payload, so this scenario
// suspends until that external callback arrives. post normalizes the callback
// payload by stripping a single surrounding pair of double-quote characters if
// present (the default callback deserializer returns raw payload text, which
// may include quotes), then appends the literal suffix "_done". Both summary
// keys report the normalized value.
//
// Expected: all SUCCEEDED, ALL_COMPLETED, cb=<payload>, post=<payload>_done,
// counts [3,0,0,3].
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "callbackdag", func(d *durable.DagBuilder) {
		pre := durable.DagStep(d, "pre", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "ready", nil })
		cb := durable.DagCallback[string](d, "cb", []durable.AnyHandle{pre},
			func(_ durable.Deps, _ durable.StepContext, _ string) error { return nil })
		durable.DagStep(d, "post", []durable.AnyHandle{cb},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				v, e := durable.Get(deps, cb)
				if e != nil {
					return "", e
				}
				return stripQuotes(v) + "_done", nil
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	cbVal, _ := durable.ResultByName[string](res, "cb")
	sum.Cb = stripQuotes(cbVal)
	sum.Post, _ = durable.ResultByName[string](res, "post")
	return sum, nil
}

// stripQuotes removes a single pair of surrounding double-quote characters from
// s if both are present. The default callback deserializer is documented as
// returning the raw payload text, which in some SDKs includes the surrounding
// quote characters; the runner's payload is alphanumeric, so stripping one
// leading and one trailing quote is unambiguous.
func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func main() { durable.Start(handler) }
