// DAG cloud-integration handlers (dag-diamond, dag-compensation,
// dag-runif, dag-wait) share this file's small, JSON-serializable
// DagSummary result type and status-collection helper.
//
// Unlike every other file in this package (each mirroring one upstream
// aws-durable-execution-conformance-tests requirement id), these four
// handlers exist to exercise this repo's OWN pkg/durable/dag primitive
// end-to-end against a REAL deployed durable Lambda, driven by
// dag_cloud_integration_test.go's CloudTestRunner-based test. They are
// registered under string ids ("dag-diamond" etc.), not the numeric
// "<suite>-<n>" ids the numbered requirement handlers use, so they never
// collide with, or get picked up by, the upstream conformance runner's
// own numbered-requirement template.
//
// DagSummary is deliberately the handler's top-level RETURN value (not
// just inspected via the operation log) so the cloud test can assert the
// DAG's semantic outcome via GetResult[DagSummary] - the same
// GetDurableExecution.Result-backed path examples/*/cloud_integration_test.go
// already rely on for a freshly-built image (see
// pkg/durable/testing/sdk_state_client.go's own doc on why a
// current-source image populates that field reliably).
package handlers

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
)

// DagSummary is the JSON-serializable outcome every DAG cloud handler
// returns. Fields are optional per-scenario (a diamond sets Merge, a
// runIf scenario sets Branch, etc.); Reason and Statuses are always set.
type DagSummary struct {
	// Reason is the DagResult.CompletionReason() string (e.g.
	// "ALL_COMPLETED", "COMPLETED_WITH_FAILURES").
	Reason string `json:"reason"`
	// Statuses maps each task name to its terminal TaskStatus string
	// ("SUCCEEDED"/"FAILED"/"SKIPPED").
	Statuses map[string]string `json:"statuses"`
	// Counts is [success, failure, skipped, total].
	Counts [4]int `json:"counts"`

	// Scenario-specific fields (zero/omitted when not applicable).
	Merge  int    `json:"merge,omitempty"`  // diamond join result
	Branch string `json:"branch,omitempty"` // runIf: the branch that ran
	Marker string `json:"marker,omitempty"` // wait: proves post-suspend replay
}

// dagStatuses collects every registered task's terminal status into a
// name->status map, plus the aggregate [success, failure, skipped, total]
// counts, from a settled DagResult.
func dagStatuses(res *dag.DagResult) (map[string]string, [4]int) {
	statuses := map[string]string{}
	for name, te := range res.Results() {
		statuses[name] = string(te.Status)
	}
	counts := [4]int{
		res.SucceededCount(), res.FailureCount(), res.SkippedCount(), res.TotalCount(),
	}
	return statuses, counts
}
