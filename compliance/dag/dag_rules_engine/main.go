// Command dag_rules_engine is a deploy-only DAG conformance handler (scenario
// 10-19) proving custom result-based completion: a rules-engine predicate
// short-circuits the moment any task's SUCCEEDED result carries a REJECT
// verdict -- expressible only because a custom ShouldComplete predicate can
// inspect task RESULTS, not just aggregate counts (which is all a threshold
// config ever sees).
//
// Graph (container "rulesengine", maxConcurrency 1): a linear chain of three
// step tasks, r1 -> r2 -> r3, each returning a verdict map. r1 -> ACCEPT,
// r2 -> REJECT, r3 (never runs) -> ACCEPT.
//
// The completion config is a custom ShouldComplete predicate, not a
// threshold: after every settlement it receives a live DagCompletionStatus
// snapshot and inspects every SUCCEEDED item's result for a REJECT verdict.
// The moment it sees one, it returns CompleteDag(OutcomeFailed). r3 is never
// started and is absent from the results map. The DAG completes with
// CustomCompletionFailed -- distinct from CompletedWithFailures, since no
// individual task FAILED. ThrowIfError() would still return an error in this
// case (the contract keys off CompletionReason too, not FailureCount alone),
// though this handler does not call it (the failure here is expected).
//
// Expected: r1 SUCCEEDED {verdict:ACCEPT}, r2 SUCCEEDED {verdict:REJECT}, r3
// absent, CUSTOM_COMPLETION_FAILED, counts [2,0,0,3]. Handler returns a
// dagsummary.Summary.
package main

import (
	"github.com/aws/aws-durable-execution-sdk-go/compliance/internal/dagsummary"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, _ struct{}) (dagsummary.Summary, error) {
	res, err := durable.Dag(ctx, "rulesengine", func(d *durable.DagBuilder) {
		r1 := durable.DagStep(d, "r1", nil,
			func(_ durable.Deps, _ durable.StepContext) (map[string]string, error) {
				return map[string]string{"verdict": "ACCEPT"}, nil
			})
		r2 := durable.DagStep(d, "r2", []durable.AnyHandle{r1},
			func(_ durable.Deps, _ durable.StepContext) (map[string]string, error) {
				return map[string]string{"verdict": "REJECT"}, nil
			})
		durable.DagStep(d, "r3", []durable.AnyHandle{r2},
			func(_ durable.Deps, _ durable.StepContext) (map[string]string, error) {
				return map[string]string{"verdict": "ACCEPT"}, nil
			})
	},
		durable.WithDagMaxConcurrency(1),
		durable.WithDagCompletion(durable.DagCompletionConfig{
			ShouldComplete: func(status durable.DagCompletionStatus) durable.CompletionDecision {
				for _, item := range status.Items {
					if item.Status != durable.StatusSucceeded {
						continue
					}
					if verdict, ok := durable.ResultOf[map[string]string](item); ok && verdict["verdict"] == "REJECT" {
						return durable.CompleteDag(durable.OutcomeFailed)
					}
				}
				return durable.ContinueDag()
			},
		}))
	if err != nil {
		return dagsummary.Summary{}, err
	}
	sum := dagsummary.From(res)
	sum.Statuses = nil // this scenario's contract returns r1/r2, not a statuses map
	sum.R1, _ = durable.ResultByName[map[string]string](res, "r1")
	sum.R2, _ = durable.ResultByName[map[string]string](res, "r2")
	return sum, nil
}

func main() { durable.Start(handler) }
