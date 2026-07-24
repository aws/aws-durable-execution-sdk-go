package durable

// CompletionOutcome is the terminal disposition a custom DAG completion
// predicate assigns to an early completion.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionOutcome int

const (
	// OutcomeSucceeded marks an early completion as a success.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	OutcomeSucceeded CompletionOutcome = iota + 1

	// OutcomeFailed marks an early completion as a failure.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	OutcomeFailed
)

// CompletionDecision is the value a DAG custom completion predicate returns
// (see [ContinueDag] / [CompleteDag]).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionDecision struct {
	complete bool
	outcome  CompletionOutcome
}

// ShouldComplete reports whether the decision asks the DAG to complete now.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (d CompletionDecision) ShouldComplete() bool { return d.complete }

// Outcome returns the completion outcome (meaningful only when
// ShouldComplete is true).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (d CompletionDecision) Outcome() CompletionOutcome { return d.outcome }

// ContinueDag returns a decision meaning "keep scheduling ready tasks".
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func ContinueDag() CompletionDecision { return CompletionDecision{} }

// CompleteDag returns a decision meaning "complete the DAG now" with the
// given outcome.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func CompleteDag(outcome CompletionOutcome) CompletionDecision {
	return CompletionDecision{complete: true, outcome: outcome}
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

// ResultOf returns the typed result of a completion item, and whether it
// was present (only succeeded items carry a result).
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
	// ShouldComplete, if set, is the results-aware custom predicate
	// (custom mode).
	ShouldComplete func(status DagCompletionStatus) CompletionDecision

	// Threshold mode (result-blind), reused from core semantics:
	MinSuccessful              *int
	ToleratedFailureCount      *int
	ToleratedFailurePercentage *float64
}

func (c *DagCompletionConfig) isCustom() bool { return c != nil && c.ShouldComplete != nil }

func (c *DagCompletionConfig) isThreshold() bool {
	return c != nil && (c.MinSuccessful != nil || c.ToleratedFailureCount != nil || c.ToleratedFailurePercentage != nil)
}

// DagCompletionReason is the reason a DAG stopped scheduling. It is a
// defined string type whose values share the underlying wire vocabulary of
// the core batch completion reasons; the DAG adds one member
// ([CompletedWithFailures]) beyond the core set.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagCompletionReason string

// Completion reasons.
const (
	// AllCompleted means every reachable task ran to a terminal state and
	// no task failed (default drain, all succeeded/skipped).
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	AllCompleted DagCompletionReason = "ALL_COMPLETED"

	// MinSuccessfulReached means a threshold completion config stopped the
	// DAG once enough tasks succeeded.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	MinSuccessfulReached DagCompletionReason = "MIN_SUCCESSFUL_REACHED"

	// FailureToleranceExceeded means a threshold completion config stopped
	// the DAG once too many tasks failed.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	FailureToleranceExceeded DagCompletionReason = "FAILURE_TOLERANCE_EXCEEDED"

	// CustomCompletionSucceeded means a custom completion predicate
	// completed the DAG with a success outcome.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	CustomCompletionSucceeded DagCompletionReason = "CUSTOM_COMPLETION_SUCCEEDED"

	// CustomCompletionFailed means a custom completion predicate completed
	// the DAG with a failure outcome.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	CustomCompletionFailed DagCompletionReason = "CUSTOM_COMPLETION_FAILED"

	// CompletedWithFailures means the DAG drained fully (default failure
	// model) but at least one reachable task failed. This is the DAG-only
	// superset member.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	CompletedWithFailures DagCompletionReason = "COMPLETED_WITH_FAILURES"
)
