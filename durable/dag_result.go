package durable

import (
	"encoding/json"
	"errors"
	"time"
)

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
	// live drain (never persisted).
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

// dagResultKind discriminates how a task's serialized result must be
// restored: a plain JSON value, a nested batch result, or a nested DAG
// result.
type dagResultKind string

const (
	dagKindPlain dagResultKind = "plain"
	dagKindBatch dagResultKind = "batch"
	dagKindDag   dagResultKind = "dag"
)

// TaskExecution records the outcome of a single task within a [DagResult].
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
	// succeeded. Typed access is via the free function Result[T] / Get[T].
	result any
	// rawResult carries the JSON-encoded result on the deserialization
	// (replay) path; unmarshaled lazily into T.
	rawResult []byte
	// kind drives recursive restore of nested batch/dag results.
	kind dagResultKind
}

// DagResult is the aggregate outcome of a DAG run: per-task executions in
// registration order, plus the completion reason. Task failures are
// reported here (via Err), not as the error return of [Dag].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagResult struct {
	tasks  []TaskExecution
	byName map[string]*TaskExecution
	reason DagCompletionReason
	// total is the number of REGISTERED tasks in the DAG: fixed,
	// independent of early completion or never-started tasks.
	total   int
	summary string // observability-only; never read on replay

	// aggregateOnly marks a result restored from an OFFLOADED (tasks-absent)
	// envelope: the per-task map is legitimately empty, but the counts below
	// are authoritative and taken verbatim from the envelope. Without it, the
	// count accessors would derive 0/0/0 from the empty task slice and
	// fabricate a zeroed success even though the checkpoint says otherwise
	// (nested-offload contract rule 1). completionReason and total are already
	// preserved from the envelope; these carry the three status counts too.
	aggregateOnly bool
	aggSucceeded  int
	aggFailed     int
	aggSkipped    int
}

func newDagResult(execs []TaskExecution, reason DagCompletionReason) *DagResult {
	r := &DagResult{tasks: execs, reason: reason, total: len(execs), byName: make(map[string]*TaskExecution, len(execs))}
	for i := range r.tasks {
		r.byName[r.tasks[i].Name] = &r.tasks[i]
	}
	return r
}

// unmarshalResult decodes a JSON-encoded task result into T. Used on the
// replay/deserialization path by [Result].
func unmarshalResult[T any](raw []byte) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, err
	}
	return v, nil
}

// Result returns the typed result of the task referenced by h. Returns
// [ErrDepNotAvailable] if the task did not succeed, or [ErrDepTypeMismatch]
// on a type/serialization edge.
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
// Same error semantics as [Result].
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

// SucceededCount returns the number of succeeded tasks. Named to match
// [BatchResult.SucceededCount] so customers see one spelling across a DAG
// result and a nested Map/Parallel BatchResult at the same call site.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) SucceededCount() int {
	if r.aggregateOnly {
		return r.aggSucceeded
	}
	return len(r.filter(StatusSucceeded))
}

// FailureCount returns the number of failed tasks.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) FailureCount() int {
	if r.aggregateOnly {
		return r.aggFailed
	}
	return len(r.filter(StatusFailed))
}

// SkippedCount returns the number of skipped tasks.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) SkippedCount() int {
	if r.aggregateOnly {
		return r.aggSkipped
	}
	return len(r.filter(StatusSkipped))
}

// TotalCount returns the number of REGISTERED tasks in the DAG. This is
// fixed and independent of early completion: never-started tasks are absent
// from the per-task executions but still counted here.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) TotalCount() int { return r.total }

// CompletionReason returns why the DAG stopped scheduling.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (r *DagResult) CompletionReason() DagCompletionReason { return r.reason }

// ThrowIfError returns a [*DagExecutionError] when at least one task failed
// or the DAG was custom-completed with a failure outcome; otherwise nil.
// Named to match [BatchResult.ThrowIfError].
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

// ── Canonical cross-SDK container envelope ─────────────────────────────────
//
// The DAG container checkpoint payload converges on ONE envelope format,
// written identically by all four SDKs (see ENVELOPE_CONVERGENCE_CONTRACT.md).
// The payload is returned by GetExecutionHistory and rendered in the AWS
// console, so it is a customer-facing contract. There is no schemaVersion;
// evolution is additive-only, so every reader MUST ignore unknown fields
// (encoding/json does by default) and treat a missing field as absent.

// dagEnvelopeType is the constant "type" discriminator every envelope carries.
const dagEnvelopeType = "DagResult"

// dagEnvelope is the canonical container payload. Field order matches the
// contract listing for console readability; conformance compares parsed
// structures, so order is not asserted. Every canonical field is ALWAYS
// present — absent values are explicit null, never omitted — with a single
// exception: `tasks` is the only OPTIONAL field, and its ABSENCE (omission)
// is the signal that the per-task detail was too large to checkpoint inline
// and now lives in the retained child operations instead.
type dagEnvelope struct {
	Type             string `json:"type"`
	TotalCount       int    `json:"totalCount"`
	SuccessCount     int    `json:"successCount"`
	FailureCount     int    `json:"failureCount"`
	SkippedCount     int    `json:"skippedCount"`
	CompletionReason string `json:"completionReason"`
	// StartedTaskNames is bounded by maxConcurrency (default 40) — only
	// running, not-yet-terminal tasks are started — so it is never dropped.
	// Always emitted as an array ([] when empty), never null.
	StartedTaskNames []string `json:"startedTaskNames"`
	// FailedTaskNames is normally an array ([] when empty). It is the only
	// aggregate that may be dropped, and only as the last size-degradation
	// step, in which case it becomes null (a nil pointer marshals to null;
	// a non-nil pointer to an empty slice marshals to []).
	FailedTaskNames *[]string `json:"failedTaskNames"`
	// Tasks is the ONLY optional field. A nil pointer is omitted entirely
	// (the offload signal, rule 2); a non-nil pointer is always emitted,
	// including an empty [].
	Tasks *[]dagEnvelopeTask `json:"tasks,omitempty"`
}

// dagEnvelopeTask is one task's entry. Every field is always present; unset
// values are explicit null.
type dagEnvelopeTask struct {
	Name        string          `json:"name"`
	Status      TaskStatus      `json:"status"`
	SkipReason  *SkipReason     `json:"skipReason"`
	ResultKind  *dagResultKind  `json:"resultKind"`
	Result      json.RawMessage `json:"result"`
	Error       *dagErrorObject `json:"error"`
	StartedAt   *string         `json:"startedAt"`
	CompletedAt *string         `json:"completedAt"`
}

// dagErrorObject is the canonical PascalCase error object shared with JS and
// Python. StackTrace is null when unavailable; Go operation errors do not
// carry one, so a nil slice (which marshals to null) is always emitted.
type dagErrorObject struct {
	ErrorType    string   `json:"ErrorType"`
	ErrorMessage string   `json:"ErrorMessage"`
	StackTrace   []string `json:"StackTrace"`
}

// dagISO8601Millis formats t as ISO 8601, UTC, millisecond precision, with a
// literal Z suffix — the timestamp shape JS already emits. Returns nil (JSON
// null) when t is the zero value (genuinely unknown).
func dagISO8601Millis(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format("2006-01-02T15:04:05.000") + "Z"
	return &s
}

// dagParseISO8601 parses a timestamp emitted by dagISO8601Millis back into a
// time.Time. Unparseable input yields the zero time.
func dagParseISO8601(s string) time.Time {
	if t, err := time.Parse("2006-01-02T15:04:05.000Z", s); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func dagOptTime(s *string) time.Time {
	if s == nil {
		return time.Time{}
	}
	return dagParseISO8601(*s)
}

// dagTaskErrorObject builds the canonical error object for a failed task. Go
// reports the SDK OPERATION error type (e.g. "StepError"), matching JS and
// Python rather than the underlying thrown error: the outer
// *DagTaskFailedError wrapper is unwrapped to the operation error it carries.
// StackTrace is always null (Go operation errors carry none).
func dagTaskErrorObject(err error) *dagErrorObject {
	if err == nil {
		return nil
	}
	cause := err
	var dtf *DagTaskFailedError
	if errors.As(err, &dtf) && dtf.Err != nil {
		cause = dtf.Err
	}
	return &dagErrorObject{
		ErrorType:    errorTypeName(cause),
		ErrorMessage: cause.Error(),
	}
}

// dagTaskResultRaw returns the task's result as a raw JSON value for the
// envelope (a nested value, not a JSON-escaped string), or nil (JSON null)
// when the task carries no result. On the live path the in-memory value is
// marshaled with encoding/json (the SDK default serdes); on the replay path
// the already-encoded rawResult bytes are reused verbatim so the value
// round-trips byte-for-byte.
func dagTaskResultRaw(te *TaskExecution) (json.RawMessage, error) {
	if len(te.rawResult) > 0 {
		return append(json.RawMessage(nil), te.rawResult...), nil
	}
	if te.result == nil {
		return nil, nil
	}
	b, err := json.Marshal(te.result)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// dagDerefKind reads a possibly-null envelope resultKind. A null kind means the
// task produced no result (FAILED or SKIPPED), so the zero value is correct.
func dagDerefKind(k *dagResultKind) dagResultKind {
	if k == nil {
		return ""
	}
	return *k
}

// dagBuildEnvelopeTask projects one TaskExecution into its envelope entry.
func dagBuildEnvelopeTask(te *TaskExecution) (dagEnvelopeTask, error) {
	// resultKind describes how to interpret Result, so it is null when there is no
	// result to interpret: a FAILED or SKIPPED task carries null for both. All four
	// SDKs agree on this (envelope contract rule 1, explicit nulls).
	var kindPtr *dagResultKind
	if te.Status == StatusSucceeded {
		kind := te.kind
		if kind == "" {
			kind = dagKindPlain
		}
		kindPtr = &kind
	}
	t := dagEnvelopeTask{
		Name:        te.Name,
		Status:      te.Status,
		ResultKind:  kindPtr,
		StartedAt:   dagISO8601Millis(te.StartedAt),
		CompletedAt: dagISO8601Millis(te.CompletedAt),
	}
	if te.Status == StatusSkipped && te.SkipReason != "" {
		sr := te.SkipReason
		t.SkipReason = &sr
	}
	if te.Status == StatusSucceeded {
		raw, err := dagTaskResultRaw(te)
		if err != nil {
			return dagEnvelopeTask{}, err
		}
		t.Result = raw
	}
	if te.Status == StatusFailed {
		t.Error = dagTaskErrorObject(te.Err)
	}
	return t, nil
}

// buildEnvelope assembles the canonical envelope from the result. includeTasks
// controls whether the per-task array is present (dropped on the offloaded
// path); includeFailedNames controls whether failedTaskNames is present
// (dropped to null only as the last size-degradation step). Counts,
// completionReason and startedTaskNames are always present.
func (r *DagResult) buildEnvelope(includeTasks, includeFailedNames bool) (dagEnvelope, error) {
	started := make([]string, 0)
	failed := make([]string, 0)
	var succ, fail, skip int
	for i := range r.tasks {
		te := &r.tasks[i]
		switch te.Status {
		case StatusSucceeded:
			succ++
		case StatusFailed:
			fail++
			failed = append(failed, te.Name)
		case StatusSkipped:
			skip++
		case StatusStarted:
			started = append(started, te.Name)
		}
	}
	env := dagEnvelope{
		Type:             dagEnvelopeType,
		TotalCount:       r.total,
		SuccessCount:     succ,
		FailureCount:     fail,
		SkippedCount:     skip,
		CompletionReason: string(r.reason),
		StartedTaskNames: started,
	}
	if includeFailedNames {
		env.FailedTaskNames = &failed
	}
	if includeTasks {
		tasks := make([]dagEnvelopeTask, 0, len(r.tasks))
		for i := range r.tasks {
			t, err := dagBuildEnvelopeTask(&r.tasks[i])
			if err != nil {
				return dagEnvelope{}, err
			}
			tasks = append(tasks, t)
		}
		env.Tasks = &tasks
	}
	return env, nil
}

// dagEnvelopePayload serializes the canonical envelope for the container
// checkpoint, degrading in the contract's exact order until it fits under the
// checkpoint size limit. It returns the payload to store and whether
// ReplayChildren must be set:
//
//  1. Full envelope WITH tasks: store inline, ReplayChildren = false.
//  2. Drop tasks: store the aggregate-only envelope AND set ReplayChildren so
//     the backend preserves the child operations that now hold the per-task
//     results.
//  3. Still too large: also drop failedTaskNames (to null). Counts,
//     completionReason and startedTaskNames are never dropped, so a DAG can
//     never fail to checkpoint because its own summary did not fit.
func dagEnvelopePayload(r *DagResult) (payload string, replayChildren bool, err error) {
	full, err := r.buildEnvelope(true, true)
	if err != nil {
		return "", false, err
	}
	b, err := json.Marshal(full)
	if err != nil {
		return "", false, err
	}
	if len(b) <= checkpointSizeLimitBytes {
		return string(b), false, nil
	}

	noTasks, err := r.buildEnvelope(false, true)
	if err != nil {
		return "", false, err
	}
	b, err = json.Marshal(noTasks)
	if err != nil {
		return "", false, err
	}
	if len(b) <= checkpointSizeLimitBytes {
		return string(b), true, nil
	}

	minimal, err := r.buildEnvelope(false, false)
	if err != nil {
		return "", false, err
	}
	b, err = json.Marshal(minimal)
	if err != nil {
		return "", false, err
	}
	return string(b), true, nil
}

// unmarshalDagEnvelope decodes a container checkpoint payload. encoding/json
// ignores unknown fields by default, satisfying the additive-only evolution
// rule: an envelope carrying an extra field deserializes without error.
func unmarshalDagEnvelope(data []byte) (*dagEnvelope, error) {
	var env dagEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	return &env, nil
}

// dagEnvelopeToResult reconstructs a *DagResult from an INLINE envelope (tasks
// present): the per-task detail is deserialized and returned directly, with no
// child reads and no re-scheduling, so task bodies never re-execute. Per-task
// results ride as rawResult so Result/ResultByName decode them lazily into the
// caller's type.
func dagEnvelopeToResult(env *dagEnvelope) *DagResult {
	var execs []TaskExecution
	if env.Tasks != nil {
		execs = make([]TaskExecution, 0, len(*env.Tasks))
		for i := range *env.Tasks {
			t := &(*env.Tasks)[i]
			te := TaskExecution{
				Name:        t.Name,
				Status:      t.Status,
				kind:        dagDerefKind(t.ResultKind),
				StartedAt:   dagOptTime(t.StartedAt),
				CompletedAt: dagOptTime(t.CompletedAt),
			}
			if te.kind == "" {
				te.kind = dagKindPlain
			}
			if t.SkipReason != nil {
				te.SkipReason = *t.SkipReason
			}
			if len(t.Result) > 0 && string(t.Result) != "null" {
				te.rawResult = append([]byte(nil), t.Result...)
			}
			if t.Error != nil {
				te.Err = &replayedError{errType: t.Error.ErrorType, message: t.Error.ErrorMessage}
			}
			execs = append(execs, te)
		}
	}
	r := newDagResult(execs, DagCompletionReason(env.CompletionReason))
	r.total = env.TotalCount
	if env.Tasks == nil {
		// Offloaded envelope: the per-task detail was too large to checkpoint
		// inline and now lives in the retained child operations, so `tasks`
		// is absent and the per-task map is legitimately empty. The three
		// counts ARE present in the envelope, so carry them verbatim rather
		// than deriving 0/0/0 from the empty slice — otherwise a nested DAG
		// that failed tasks would be restored as a zeroed success and
		// ThrowIfError would wrongly report success (contract rule 1).
		r.aggregateOnly = true
		r.aggSucceeded = env.SuccessCount
		r.aggFailed = env.FailureCount
		r.aggSkipped = env.SkippedCount
	}
	return r
}

// dagApplyOffloadEnvelope overlays the offloaded (tasks-absent) envelope's
// authoritative aggregate onto executions reconstructed by re-running the
// scheduler. Per-task RESULTS come from each task's own child checkpoint (the
// scheduler fast-path), but the STARTED set, completionReason and total come
// from the envelope: the started set records tasks that were in-flight at an
// early completion and is NOT recoverable from any child operation, so
// re-deriving it from a fresh, timing-dependent re-run would be
// non-deterministic. Taking it from startedTaskNames fixes that.
func dagApplyOffloadEnvelope(execs []TaskExecution, env *dagEnvelope, defs []*dagTaskDef) []TaskExecution {
	if env == nil {
		return execs
	}
	started := make(map[string]struct{}, len(env.StartedTaskNames))
	for _, n := range env.StartedTaskNames {
		started[n] = struct{}{}
	}
	present := make(map[string]struct{}, len(execs))
	for i := range execs {
		present[execs[i].Name] = struct{}{}
		if _, ok := started[execs[i].Name]; ok {
			execs[i].Status = StatusStarted
			execs[i].result = nil
			execs[i].rawResult = nil
			execs[i].Err = nil
		}
	}
	for _, n := range env.StartedTaskNames {
		if _, ok := present[n]; !ok {
			execs = append(execs, TaskExecution{Name: n, Status: StatusStarted, kind: dagKindFor(defs, n)})
		}
	}
	return execs
}
