// Command dag_compensation is a deploy-only DAG conformance handler
// exercising trigger-rule compensation (saga): an upstream charge fails and
// downstream tasks react via trigger rules. The DAG container SUCCEEDS while
// the failure is reported inside the result. It returns a dagsummary.Summary
// as its top-level result for cloud assertion.
//
// Graph: charge(FAILS) -> {fulfill (ALL_SUCCESS->skip), refund (ALL_FAILED),
// audit (ALL_DONE)}. Expected: fulfill SKIPPED, refund+audit SUCCEEDED,
// charge FAILED, COMPLETED_WITH_FAILURES, counts [2,1,1,4].
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "compensation", func(d *durable.DagBuilder) {
		charge := durable.DagStep(d, "charge", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "", errors.New("payment declined")
			})
		durable.DagStep(d, "fulfill", []durable.AnyHandle{charge},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "shipped", nil })
		durable.DagStep(d, "refund", []durable.AnyHandle{charge},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "refunded", nil }).
			WithTrigger(durable.AllFailed)
		durable.DagStep(d, "audit", []durable.AnyHandle{charge},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "logged", nil }).
			WithTrigger(durable.AllDone)
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	// The failed charge is expected and compensated; do not ThrowIfError.
	return dagsummary.From(res), nil
}

func main() { durable.Start(handler) }
