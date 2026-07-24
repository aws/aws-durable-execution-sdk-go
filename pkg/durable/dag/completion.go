package dag

import "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"

// CompletionReason is the reason a DAG stopped scheduling. It shares the
// underlying string type with the core batch completion reasons; the DAG
// adds one member (CompletedWithFailures) beyond the core set, preserving
// the dag -> core (never core -> dag) dependency direction.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionReason = string

// Completion reasons. The three threshold reasons and the two custom
// reasons are re-exported from the core operations package (single source
// of truth); CompletedWithFailures is DAG-only.
const (
	// AllCompleted means every reachable task ran to a terminal state and
	// no task failed (default drain, all succeeded/skipped).
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	AllCompleted CompletionReason = operations.CompletionReasonAllCompleted

	// MinSuccessfulReached means a threshold completion config stopped the
	// DAG once enough tasks succeeded.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	MinSuccessfulReached CompletionReason = operations.CompletionReasonMinSuccessfulReached

	// FailureToleranceExceeded means a threshold completion config stopped
	// the DAG once too many tasks failed.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	FailureToleranceExceeded CompletionReason = operations.CompletionReasonFailureToleranceExceeded

	// CustomCompletionSucceeded means a custom completion predicate
	// completed the DAG with a success outcome.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	CustomCompletionSucceeded CompletionReason = operations.CustomCompletionSucceeded

	// CustomCompletionFailed means a custom completion predicate completed
	// the DAG with a failure outcome.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	CustomCompletionFailed CompletionReason = operations.CustomCompletionFailed

	// CompletedWithFailures means the DAG drained fully (default failure
	// model) but at least one reachable task failed. This is the DAG-only
	// superset member.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	CompletedWithFailures CompletionReason = "COMPLETED_WITH_FAILURES"
)
