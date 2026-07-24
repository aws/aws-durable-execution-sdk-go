// Package dagsummary provides a small, JSON-serializable projection of a
// [durable.DagResult] shared by the DAG conformance handlers. The cloud
// integration stage asserts against this shape as each handler's top-level
// result, so it captures the terminal completion reason, per-task statuses,
// and the [succeeded, failed, skipped, total] counts.
package dagsummary

import "github.com/aws/aws-durable-execution-sdk-go/durable"

// Summary is the deploy-and-assert projection of a DAG run.
type Summary struct {
	// Reason is the DAG's completion reason (e.g. ALL_COMPLETED,
	// COMPLETED_WITH_FAILURES).
	Reason string `json:"reason"`
	// Statuses maps each task name to its terminal status.
	Statuses map[string]string `json:"statuses"`
	// Counts is [succeeded, failed, skipped, total].
	Counts [4]int `json:"counts"`
	// Merge carries the diamond merge result (diamond handler only).
	Merge int `json:"merge,omitempty"`
	// Branch names the runIf branch that ran (runif handler only).
	Branch string `json:"branch,omitempty"`
	// Marker proves post-resume replay (wait handler only).
	Marker string `json:"marker,omitempty"`
}

// From builds a Summary from a drained DagResult.
func From(res *durable.DagResult) Summary {
	statuses := make(map[string]string, len(res.Results()))
	for name, te := range res.Results() {
		statuses[name] = string(te.Status)
	}
	return Summary{
		Reason:   string(res.CompletionReason()),
		Statuses: statuses,
		Counts:   [4]int{res.SucceededCount(), res.FailureCount(), res.SkippedCount(), res.TotalCount()},
	}
}
