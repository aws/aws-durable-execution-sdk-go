package dag

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// CompletionOutcome is re-exported from the core types package: the
// terminal disposition (OutcomeSucceeded/OutcomeFailed) a custom completion
// predicate assigns.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionOutcome = types.CompletionOutcome

const (
	// OutcomeSucceeded marks an early completion as a success.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	OutcomeSucceeded = types.OutcomeSucceeded

	// OutcomeFailed marks an early completion as a failure.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	OutcomeFailed = types.OutcomeFailed
)

// CompletionDecision is the opaque value a DAG custom completion predicate
// returns (see ContinueDag / CompleteDag).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionDecision = types.CompletionDecision

// ContinueDag returns a decision meaning "keep scheduling ready tasks".
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func ContinueDag() CompletionDecision { return types.NewContinueDecision() }

// CompleteDag returns a decision meaning "complete the DAG now" with the
// given outcome.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func CompleteDag(outcome CompletionOutcome) CompletionDecision {
	return types.NewCompleteDecision(outcome)
}

// DagCompletionItemStatus is a results-aware, SKIPPED-aware projection of a
// single task's terminal state, supplied to a custom completion predicate.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagCompletionItemStatus struct {
	Name       string
	Status     TaskStatus
	SkipReason SkipReason
	result     any
}

// ResultOf returns the typed result of a completion item, and whether it was
// present (only succeeded items carry a result).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func ResultOf[T any](s DagCompletionItemStatus) (T, bool) {
	var zero T
	if s.result == nil {
		return zero, false
	}
	v, ok := s.result.(T)
	return v, ok
}

// DagCompletionStatus is the results-aware status snapshot passed to a
// custom ShouldComplete predicate each time a task settles.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagCompletionStatus struct {
	SuccessCount, FailureCount, SkippedCount, CompletedCount, TotalCount int
	// Items lists every settled task in registration order.
	Items []DagCompletionItemStatus
	// Results maps terminal task name -> its status.
	Results map[string]DagCompletionItemStatus
}

// DagCompletionConfig configures DAG completion. Exactly one mode may be
// set: a custom ShouldComplete predicate, OR the threshold fields (reused
// from core batch semantics). Mutual exclusivity is enforced at run time.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagCompletionConfig struct {
	// ShouldComplete, if set, is the results-aware custom predicate (custom
	// mode).
	ShouldComplete func(status DagCompletionStatus) CompletionDecision

	// Threshold mode (result-blind), reused from core semantics:
	MinSuccessful              *int
	ToleratedFailureCount      *int
	ToleratedFailurePercentage *float64
}

// isCustom reports whether the config uses the custom predicate mode.
func (c *DagCompletionConfig) isCustom() bool { return c != nil && c.ShouldComplete != nil }

// isThreshold reports whether any threshold field is set.
func (c *DagCompletionConfig) isThreshold() bool {
	return c != nil && (c.MinSuccessful != nil || c.ToleratedFailureCount != nil || c.ToleratedFailurePercentage != nil)
}

// CompletionReason is the reason a DAG stopped scheduling. It is a defined
// string type (matching the TriggerRule/TaskStatus/SkipReason pattern) whose
// values share the underlying wire vocabulary of the core batch completion
// reasons; the DAG adds one member (CompletedWithFailures) beyond the core
// set, preserving the dag -> core (never core -> dag) dependency direction.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionReason string

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
