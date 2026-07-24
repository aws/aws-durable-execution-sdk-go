package dag

import "time"

// TaskStatus is the terminal (or in-flight, for early completion) status of
// a single DAG task.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type TaskStatus string

const (
	// StatusSucceeded means the task ran and returned without error.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	StatusSucceeded TaskStatus = "SUCCEEDED"

	// StatusFailed means the task ran and returned an error.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	StatusFailed TaskStatus = "FAILED"

	// StatusSkipped means the task did not run because its trigger rule or
	// runIf predicate was not satisfied.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	StatusSkipped TaskStatus = "SKIPPED"

	// StatusStarted means the task's goroutine was launched but the DAG
	// completed early before it settled; only meaningful within a single
	// live drain (never persisted). See DAG_SPEC_GO.md §5.6.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	StatusStarted TaskStatus = "STARTED"
)

// SkipReason explains why a task was skipped.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type SkipReason string

const (
	// SkipTriggerRule means the task's upstream statuses did not satisfy
	// its trigger rule.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	SkipTriggerRule SkipReason = "TRIGGER_RULE"

	// SkipRunIf means the task's runIf predicate returned false.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	SkipRunIf SkipReason = "RUN_IF_PREDICATE"
)

// resultKind discriminates how a task's serialized result must be restored:
// a plain JSON value, a nested batch result, or a nested DAG result.
type resultKind string

const (
	kindPlain resultKind = "plain"
	kindBatch resultKind = "batch"
	kindDag   resultKind = "dag"
)

// TaskExecution records the outcome of a single task within a DagResult.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type TaskExecution struct {
	// Name is the task's name.
	Name string
	// Status is the task's terminal (or STARTED) status.
	Status TaskStatus
	// SkipReason is set only when Status == StatusSkipped.
	SkipReason SkipReason
	// Err is set only when Status == StatusFailed.
	Err error
	// StartedAt/CompletedAt are observability timestamps.
	StartedAt   time.Time
	CompletedAt time.Time

	// result is the in-memory result value, present only when the task
	// succeeded. Unexported: typed access is via the free function
	// Result[T] / Get[T], which lazily unmarshal from rawResult when
	// needed (replay path).
	result any
	// rawResult carries the JSON-encoded result on the deserialization
	// (replay) path; unmarshaled lazily into T.
	rawResult []byte
	// kind drives recursive restore of nested batch/dag results.
	kind resultKind
}
