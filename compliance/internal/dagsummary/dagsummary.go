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
	// Statuses maps each task name to its terminal status. Emitted by every
	// DAG handler except large-payload (10-15), whose contract fixes the
	// returned summary to exactly {reason, counts, digestBefore, digestAfter,
	// match}; that handler leaves this nil so omitempty drops the key.
	Statuses map[string]string `json:"statuses,omitempty"`
	// Counts is [succeeded, failed, skipped, total].
	Counts [4]int `json:"counts"`
	// Merge carries the merge result of a fan-in task. It is an int for the
	// diamond handler (merge=31) and a string for the concurrent handlers
	// (overlap merge="SsFf", suspend merge="SF"), so it is typed as any to
	// serialize either under the single "merge" key the language-neutral
	// YAML asserts against.
	Merge any `json:"merge,omitempty"`
	// PeakConcurrency is the maximum number of instrumented tasks observed
	// running at once (concurrent-overlap handler only). It proves the DAG
	// genuinely overlapped tasks rather than serializing them.
	PeakConcurrency int `json:"peakConcurrency,omitempty"`
	// Branch names the runIf branch that ran (runif handler only).
	Branch string `json:"branch,omitempty"`
	// Marker proves post-resume replay (wait handler only).
	Marker string `json:"marker,omitempty"`
	// Group carries the child task result (childdag handler only).
	Group int `json:"group,omitempty"`
	// Done carries the downstream step result (childdag/wfcdag handlers).
	Done int `json:"done,omitempty"`
	// Sum carries the map aggregation result (mapdag handler only).
	Sum int `json:"sum,omitempty"`
	// Join carries the parallel join result (paralleldag handler only).
	Join string `json:"join,omitempty"`
	// Audit carries the compensation task's dependency-read verdict
	// ("absent" or "present") for a failed upstream (compensate handler,
	// 10-18, only). It proves a compensation task observes a FAILED
	// dependency's result as absent rather than a stale/fabricated value.
	Audit string `json:"audit,omitempty"`
	// R1 and R2 carry the first two tasks' verdict results (rules-engine
	// handler, 10-19, only). r3's absence (never started) is proven by
	// Counts' total=3/succeeded=2 rather than a field here, matching the
	// catalog rule that a never-started task is omitted entirely.
	R1 map[string]string `json:"r1,omitempty"`
	R2 map[string]string `json:"r2,omitempty"`
	// Poll carries the final waitForCondition state (wfcdag handler only).
	Poll int `json:"poll,omitempty"`
	// Post carries a downstream step result. It is an int for the outerdag
	// handler (post=50) and a string for the callbackdag handler
	// (post="<payload>_done"), so it is typed as any to serialize either.
	Post any `json:"post,omitempty"`
	// Call carries the invoke task result (invokedag handler only).
	Call int `json:"call,omitempty"`
	// Cb carries the callback task result (callbackdag handler only).
	Cb string `json:"cb,omitempty"`
	// DigestBefore is a language-neutral fingerprint of the aggregate
	// DagResult computed BEFORE the mid-handler suspend
	// ("<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>"),
	// large-payload handler only.
	DigestBefore string `json:"digestBefore,omitempty"`
	// DigestAfter is the same fingerprint recomputed from the REPLAYED
	// DagResult after the suspend, large-payload handler only. Equality with
	// DigestBefore proves the >256KB aggregate survived the offload and came
	// back identical through the SDK's replay strategy (child-body
	// re-execution for Go).
	DigestAfter string `json:"digestAfter,omitempty"`
	// Match reports whether DigestBefore == DigestAfter. It is a *bool so
	// the key is emitted (true or false) only by the large-payload handler
	// and omitted entirely by every other handler.
	Match *bool `json:"match,omitempty"`
	// InnerReason is the nested inner DAG's completion reason, emitted only by
	// the nested-large-payload handler (10-17). Under the nested-offload bug
	// it would still read ALL_COMPLETED from a fabricated inner result, which
	// is why the digest — not the reason — is that scenario's decisive check.
	InnerReason string `json:"innerReason,omitempty"`
	// InnerCounts is the inner DAG's [total, failed, skipped, succeeded],
	// emitted only by the nested-large-payload handler (10-17). Note the
	// ordering deliberately differs from Counts ([succeeded, failed, skipped,
	// total]): the 10-17 contract fixes innerCounts as
	// [total, failed, skipped, succeeded]. A pointer so the key is emitted
	// only by that handler.
	InnerCounts *[4]int `json:"innerCounts,omitempty"`
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
