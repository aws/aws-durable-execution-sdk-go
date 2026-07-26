// Command dag_run_if_abort is a deploy-only DAG conformance handler
// exercising the runIf abort path (RUNIF_ABORT_CONTRACT): a task's runIf
// predicate throws, which is a defect in deterministic code, so the whole
// DAG ABORTS with a typed *DagPredicateError rather than recording the task
// as FAILED and driving downstream compensation. Because a runIf in Go is a
// pure func(Deps) bool with no error return, "throws" is expressed as a
// panic; the SDK recovers it (it never reaches the runtime) and converts it
// into the typed abort error.
//
// Graph (abortdag, maxConcurrency 1):
//
//	gate(=1) -> guarded[runIf panics "predicate boom"] -.after-> refund(ALL_FAILED)
//
// Expected: the execution FAILS. gate SUCCEEDED; guarded's body never runs
// (no terminal state); refund never runs (a predicate defect must not drive
// ALL_FAILED compensation). Dag returns a *DagPredicateError naming
// "guarded", which this handler propagates so the invocation fails.
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "abortdag", func(d *durable.DagBuilder) {
		gate := durable.DagStep(d, "gate", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
		guarded := durable.DagStep(d, "guarded", []durable.AnyHandle{gate},
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "ran", nil },
			durable.WithRunIf(func(durable.Deps) bool { panic(errors.New("predicate boom")) }))
		durable.DagStep(d, "refund", nil,
			func(_ durable.Deps, _ durable.StepContext) (string, error) { return "refunded", nil }).
			After(guarded).
			WithTrigger(durable.AllFailed)
	}, durable.WithDagMaxConcurrency(1))
	// A panicking runIf ABORTS the DAG: Dag returns a typed
	// *DagPredicateError and no result. Propagate it so the execution FAILS
	// on the wire — that failure is the whole point of this scenario.
	if err != nil {
		return dagsummary.Summary{}, err
	}
	// Unreachable under the abort contract; kept so a regression that
	// downgraded the abort to a task failure surfaces as a wrong (non-error)
	// outcome rather than a nil-deref.
	return dagsummary.From(res), nil
}

func main() { durable.Start(handler) }
