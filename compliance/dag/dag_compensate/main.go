// Command dag_compensate is a deploy-only DAG conformance handler (scenario
// 10-18) proving the deps-nullability contract: a compensation task that reads
// its inline dependency's result for a FAILED upstream MUST observe it as
// absent, never as a stale or fabricated present value.
//
// Graph (container "compensate", maxConcurrency 1):
//   - charge: a root step that ALWAYS fails. Its per-task retry strategy is
//     disabled (a single attempt) so it ends FAILED deterministically,
//     producing exactly one StepFailed.
//   - audit: a step depending on charge via an INLINE (typed) dep and using the
//     AllDone trigger, so it runs even though charge FAILED and receives charge
//     in its resolved Deps. durable.Get(deps, charge) returns ErrDepNotAvailable
//     for a dependency that did not SUCCEED, so audit returns "absent"; it would
//     return "present" only if the value resolved.
//
// Expected: charge FAILED, audit SUCCEEDED ("absent"), COMPLETED_WITH_FAILURES,
// counts [1,1,0,2]. The DAG container SUCCEEDS (the failure is compensated, so
// the handler does NOT ThrowIfError). Handler returns a dagsummary.Summary.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "compensate", func(d *durable.DagBuilder) {
		charge := durable.DagStep(d, "charge", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "", errors.New("charge failed")
			},
			// Single attempt (no retry) so charge ends FAILED deterministically.
			durable.WithTaskRetry(func(_ error, _ int) durable.RetryDecision {
				return durable.RetryDecision{Retry: false}
			}))
		durable.DagStep(d, "audit", []durable.AnyHandle{charge},
			func(deps durable.Deps, _ durable.StepContext) (string, error) {
				// A failed dependency's result is absent (ErrDepNotAvailable),
				// never a stale/fabricated value.
				if _, gerr := durable.Get(deps, charge); errors.Is(gerr, durable.ErrDepNotAvailable) {
					return "absent", nil
				}
				return "present", nil
			}).
			WithTrigger(durable.AllDone)
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	// The failed charge is expected and compensated; do not ThrowIfError.
	sum := dagsummary.From(res)
	sum.Audit, _ = durable.ResultByName[string](res, "audit")
	return sum, nil
}

func main() { durable.Start(handler) }
