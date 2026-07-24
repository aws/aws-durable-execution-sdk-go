package types

// This file adds the shared, additive value types that a results-aware
// custom completion predicate needs: a completion OUTCOME, an opaque
// completion DECISION, and a per-item status projection that (unlike the
// threshold-only CompletionConfig) can carry each item's RESULT and a
// SKIPPED status. These are placed in the core types package so that the
// batch operations (Map/Parallel) could adopt the same results-aware
// predicate in future; the experimental DAG feature (pkg/durable/dag)
// consumes them today via its own completion loop. See DAG_SPEC_GO.md
// §2.10 and §15(1).

// CompletionOutcome is the terminal disposition a custom completion
// predicate assigns when it decides to complete a batch/DAG early:
// OutcomeSucceeded or OutcomeFailed.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionOutcome int

const (
	// OutcomeSucceeded marks an early completion as a success.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	OutcomeSucceeded CompletionOutcome = iota

	// OutcomeFailed marks an early completion as a failure.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	OutcomeFailed
)

// String returns a stable, wire-friendly name for the outcome.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (o CompletionOutcome) String() string {
	switch o {
	case OutcomeSucceeded:
		return "SUCCEEDED"
	case OutcomeFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// CompletionDecision is the opaque value a custom completion predicate
// returns: either "keep scheduling" (see NewContinueDecision) or "stop now
// with this outcome" (see NewCompleteDecision). It is deliberately opaque -
// callers build it only through the two constructors and read it only
// through its accessors - so the set of valid decisions stays closed even
// though Go cannot express a closed union type.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionDecision struct {
	complete bool
	outcome  CompletionOutcome
}

// NewContinueDecision returns a decision meaning "do not complete yet;
// keep scheduling ready work."
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func NewContinueDecision() CompletionDecision { return CompletionDecision{complete: false} }

// NewCompleteDecision returns a decision meaning "complete now" with the
// given outcome.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func NewCompleteDecision(outcome CompletionOutcome) CompletionDecision {
	return CompletionDecision{complete: true, outcome: outcome}
}

// ShouldComplete reports whether this decision requests early completion.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (d CompletionDecision) ShouldComplete() bool { return d.complete }

// Outcome returns the completion outcome; it is meaningful only when
// ShouldComplete reports true.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (d CompletionDecision) Outcome() CompletionOutcome { return d.outcome }

// CompletionItemStatus is a results-aware, SKIPPED-aware projection of a
// single item's terminal state, supplied to a custom completion predicate.
// Unlike the threshold-only CompletionConfig, it carries the item's own
// Result (present only when Succeeded) and can represent a Skipped item -
// the two pieces of information a results-driven predicate needs and the
// batch status historically lacked.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CompletionItemStatus struct {
	// Name is the item/task name.
	Name string
	// Status is the item's terminal status string (e.g. "SUCCEEDED",
	// "FAILED", "SKIPPED", "STARTED"); the concrete enum lives in the
	// consuming package (dag.TaskStatus).
	Status string
	// Skipped reports whether the item was skipped (never executed).
	Skipped bool
	// Result is the item's in-memory result, present only when the item
	// succeeded; nil otherwise.
	Result any
}

// Succeeded reports whether the item completed successfully (its Result is
// then populated).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (s CompletionItemStatus) Succeeded() bool { return s.Status == "SUCCEEDED" }
