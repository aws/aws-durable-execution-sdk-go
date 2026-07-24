// Command dag-compensation demonstrates the EXPERIMENTAL [durable.Dag]
// trigger-rule compensation (saga) pattern: an upstream task fails, and
// downstream tasks react via trigger rules. The DAG container itself
// SUCCEEDS even though a task failed — individual failures are reported
// inside the DagResult, and the completion reason is COMPLETED_WITH_FAILURES.
//
// Graph: charge(FAILS) -> fulfill (ALL_SUCCESS -> skipped), refund
// (ALL_FAILED -> runs, the compensation), audit (ALL_DONE -> runs regardless).
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// compensationResult is the JSON-serializable projection of the DagResult.
type compensationResult struct {
	Statuses  map[string]string `json:"statuses"`
	Succeeded int               `json:"succeeded"`
	Failed    int               `json:"failed"`
	Skipped   int               `json:"skipped"`
	Total     int               `json:"total"`
	Reason    string            `json:"reason"`
}

func handler(ctx durable.Context, _ any) (compensationResult, error) {
	res, err := durable.Dag(ctx, "compensation", func(d *durable.DagBuilder) {
		charge := durable.DagStep(d, "charge", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "", errors.New("payment declined")
			})
		// Only runs if the charge succeeded — it did not, so this is skipped.
		durable.DagStep(d, "fulfill", []durable.AnyHandle{charge},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "shipped", nil })
		// Compensation: runs only when every upstream failed.
		durable.DagStep(d, "refund", []durable.AnyHandle{charge},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "refunded", nil }).
			WithTrigger(durable.AllFailed)
		// Audit runs once the upstream is terminal, whatever the outcome.
		durable.DagStep(d, "audit", []durable.AnyHandle{charge},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "logged", nil }).
			WithTrigger(durable.AllDone)
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return compensationResult{}, err
	}
	// NOTE: we deliberately do NOT call res.ThrowIfError(): the failed charge
	// is expected, and the compensation path handled it. The DAG container
	// succeeds; task-level failures are surfaced via the result.
	statuses := make(map[string]string, len(res.Results()))
	for name, te := range res.Results() {
		statuses[name] = string(te.Status)
	}
	return compensationResult{
		Statuses:  statuses,
		Succeeded: res.SucceededCount(),
		Failed:    res.FailureCount(),
		Skipped:   res.SkippedCount(),
		Total:     res.TotalCount(),
		Reason:    string(res.CompletionReason()),
	}, nil
}

func main() { durable.Start(handler) }
