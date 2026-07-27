// Command dag_retry is a deploy-only DAG conformance handler (scenario
// 10-16) proving that a per-task retry strategy recovers a flaky task INSIDE
// a DAG and that the recovered result flows downstream normally. Nothing
// else in the suite covers retry inside a DAG.
//
// Graph (container "retrydag", maxConcurrency 1):
//   - flaky: a step with a per-task retry strategy allowing at least 3
//     attempts and no backoff. It reads its attempt number from the step
//     context (StepContext.Attempt(), which is 1-indexed at runtime) and
//     throws until the third attempt, on which it returns the attempt
//     number (3).
//   - after: a step depending on flaky that returns flaky's result doubled
//     (6), proving a retried task's result flows downstream normally.
//
// Expected: flaky SUCCEEDED (not FAILED) and after SUCCEEDED (runs, not
// skipped), ALL_COMPLETED, counts 2/2/0/0. Handler returns
// {"flaky": 3, "after": 6}.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// retryOut is the handler's top-level result. The field names/values are a
// shared cross-language expectation pinned by the conformance orchestrator.
type retryOut struct {
	Flaky int `json:"flaky"`
	After int `json:"after"`
}

func handler(ctx durable.Context, _ struct{}) (retryOut, error) {
	res, err := durable.Dag(ctx, "retrydag", func(d *durable.DagBuilder) {
		flaky := durable.DagStep(d, "flaky", nil,
			func(_ durable.Deps, sc durable.StepContext) (int, error) {
				// Attempt() is 1-indexed at runtime: 1, 2, 3, ...
				if sc.Attempt() < 3 {
					return 0, errors.New("flaky: not yet third attempt")
				}
				return sc.Attempt(), nil
			},
			durable.WithTaskRetry(func(_ error, attempt int) durable.RetryDecision {
				// Retry after attempts 1 and 2, allowing a 3rd attempt; no
				// backoff delay worth waiting on.
				return durable.RetryDecision{Retry: attempt < 3, Delay: 0}
			}))
		durable.DagStep(d, "after", []durable.AnyHandle{flaky},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, e := durable.Get(deps, flaky)
				return v * 2, e
			})
	}, durable.WithDagMaxConcurrency(1))
	if err != nil {
		return retryOut{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return retryOut{}, err
	}
	f, err := durable.ResultByName[int](res, "flaky")
	if err != nil {
		return retryOut{}, err
	}
	a, err := durable.ResultByName[int](res, "after")
	if err != nil {
		return retryOut{}, err
	}
	return retryOut{Flaky: f, After: a}, nil
}

func main() { durable.Start(handler) }
