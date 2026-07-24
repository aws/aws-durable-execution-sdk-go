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

// DagResult is the aggregate outcome of a DAG run: per-task executions in
// registration order, plus the completion reason. Task failures are
// reported here (via Err), not as the error return of Dag(...).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagResult struct {
	tasks  []TaskExecution
	byName map[string]*TaskExecution
	reason CompletionReason
	// total is the number of REGISTERED tasks in the DAG (spec §2.8):
	// fixed, independent of early completion or never-started tasks. It is
	// deliberately decoupled from len(tasks): under early completion,
	// never-started tasks are absent from tasks (§9.6) but still count
	// toward total. Defaults to len(execs); the scheduler/replay paths
	// override it with the registered count.
	total   int
	summary string // observability-only; never read on replay
}

func newDagResult(execs []TaskExecution, reason CompletionReason) *DagResult {
	r := &DagResult{tasks: execs, reason: reason, total: len(execs), byName: make(map[string]*TaskExecution, len(execs))}
	for i := range r.tasks {
		r.byName[r.tasks[i].Name] = &r.tasks[i]
	}
	return r
}

// Result returns the typed result of the task referenced by h. Returns
// ErrDepNotAvailable if the task did not succeed, or ErrDepTypeMismatch on a
// type/serialization edge.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func Result[T any](r *DagResult, h TaskHandle[T]) (T, error) {
	var zero T
	te, ok := r.byName[h.name]
	if !ok || te.Status != StatusSucceeded {
		return zero, ErrDepNotAvailable
	}
	if te.result != nil {
		v, ok := te.result.(T)
		if !ok {
			return zero, ErrDepTypeMismatch
		}
		return v, nil
	}
	// Lazy unmarshal from rawResult (replay path).
	if len(te.rawResult) > 0 {
		v, err := unmarshalResult[T](te.rawResult)
		if err != nil {
			return zero, err
		}
		te.result = v
		return v, nil
	}
	return zero, ErrDepNotAvailable
}

// ResultByName returns the typed result of the task with the given name.
// It is a convenience over Result[T] for call sites that hold only the name
// (e.g. after the register callback has returned). Same error semantics as
// Result.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func ResultByName[T any](r *DagResult, name string) (T, error) {
	return Result(r, TaskHandle[T]{name: name})
}

// Status returns the status of a task by name or handle, and false if the
// task never started (absent from results).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) Status(nameOrHandle any) (TaskStatus, bool) {
	name := ""
	switch v := nameOrHandle.(type) {
	case string:
		name = v
	case AnyHandle:
		name = v.taskName()
	default:
		return "", false
	}
	te, ok := r.byName[name]
	if !ok {
		return "", false
	}
	return te.Status, true
}

func (r *DagResult) filter(status TaskStatus) []TaskExecution {
	var out []TaskExecution
	for _, te := range r.tasks {
		if te.Status == status {
			out = append(out, te)
		}
	}
	return out
}

// Succeeded returns the succeeded task executions.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) Succeeded() []TaskExecution { return r.filter(StatusSucceeded) }

// Failed returns the failed task executions.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) Failed() []TaskExecution { return r.filter(StatusFailed) }

// Skipped returns the skipped task executions.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) Skipped() []TaskExecution { return r.filter(StatusSkipped) }

// Results returns a copy of all task executions keyed by name.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) Results() map[string]TaskExecution {
	out := make(map[string]TaskExecution, len(r.tasks))
	for _, te := range r.tasks {
		out[te.Name] = te
	}
	return out
}

// SucceededCount returns the number of succeeded tasks. Named to match the
// base BatchResult.SucceededCount() so customers see one spelling across a
// DAG result and a nested Map/Parallel BatchResult at the same call site.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) SucceededCount() int { return len(r.filter(StatusSucceeded)) }

// FailureCount returns the number of failed tasks.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) FailureCount() int { return len(r.filter(StatusFailed)) }

// SkippedCount returns the number of skipped tasks.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) SkippedCount() int { return len(r.filter(StatusSkipped)) }

// TotalCount returns the number of REGISTERED tasks in the DAG (spec §2.8).
// This is fixed and independent of early completion: never-started tasks are
// absent from the per-task executions (§9.6) but still counted here.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) TotalCount() int { return r.total }

// CompletionReason returns why the DAG stopped scheduling.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) CompletionReason() CompletionReason { return r.reason }

// ThrowIfError returns a *DagExecutionError when at least one task failed or
// the DAG was custom-completed with a failure outcome; otherwise nil. Named
// to match the base BatchResult.ThrowIfError() so customers see one spelling
// across a DAG result and a nested Map/Parallel BatchResult at the same call
// site.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) ThrowIfError() error {
	if r.FailureCount() == 0 && r.reason != CustomCompletionFailed {
		return nil
	}
	firstFailed := ""
	var cause error
	for _, te := range r.tasks {
		if te.Status == StatusFailed {
			firstFailed = te.Name
			cause = te.Err
			break
		}
	}
	return &DagExecutionError{FirstFailed: firstFailed, cause: cause}
}
