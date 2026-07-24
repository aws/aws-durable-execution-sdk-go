package dag

import (
	"errors"
	"fmt"
	"strings"
)

// DagValidationError aggregates all registration/validation problems
// detected before any task is scheduled. Individual errors are available
// via Errs and via errors.As on the wrapped members.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagValidationError struct{ Errs []error }

func (e *DagValidationError) Error() string {
	parts := make([]string, len(e.Errs))
	for i, err := range e.Errs {
		parts[i] = err.Error()
	}
	return fmt.Sprintf("dag validation failed: %s", strings.Join(parts, "; "))
}

// Unwrap exposes the aggregated errors for errors.Is/errors.As traversal.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (e *DagValidationError) Unwrap() []error { return e.Errs }

// DagInvalidTaskNameError reports a task name that violates the naming
// rules (non-empty, <=100 chars, ^[a-zA-Z0-9_]+$, no "DAG_NODE_T_"
// substring).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagInvalidTaskNameError struct{ Name, Reason string }

func (e *DagInvalidTaskNameError) Error() string {
	return fmt.Sprintf("invalid task name %q: %s", e.Name, e.Reason)
}

// DagDuplicateTaskError reports two tasks registered under the same name.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagDuplicateTaskError struct{ Name string }

func (e *DagDuplicateTaskError) Error() string {
	return fmt.Sprintf("duplicate task name %q", e.Name)
}

// DagInvalidDependencyError reports a dependency on a task that is not
// registered in the same scope.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagInvalidDependencyError struct{ Task, Dep string }

func (e *DagInvalidDependencyError) Error() string {
	return fmt.Sprintf("task %q depends on unknown/foreign task %q", e.Task, e.Dep)
}

// DagCyclicDependencyError reports a dependency cycle; Cycle lists the task
// names on the cycle.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagCyclicDependencyError struct{ Cycle []string }

func (e *DagCyclicDependencyError) Error() string {
	if len(e.Cycle) == 0 {
		return "cyclic dependency detected"
	}
	return fmt.Sprintf("cyclic dependency detected: %s", strings.Join(e.Cycle, " -> "))
}

// DagInvalidConfigError reports an invalid DAG/task configuration (e.g.
// non-positive maxConcurrency, or mutually-exclusive completion config).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagInvalidConfigError struct{ Reason string }

func (e *DagInvalidConfigError) Error() string {
	return fmt.Sprintf("invalid dag config: %s", e.Reason)
}

// DagInvalidTriggerRuleError reports an unknown trigger rule.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagInvalidTriggerRuleError struct{ Rule TriggerRule }

func (e *DagInvalidTriggerRuleError) Error() string {
	return fmt.Sprintf("invalid trigger rule %q", string(e.Rule))
}

// DagExecutionError is returned by DagResult.Err() when at least one task
// failed (or a custom predicate completed the DAG with a failure outcome).
// It wraps the first failed task's cause, so errors.Is/errors.As traverse
// into the underlying error.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagExecutionError struct {
	FirstFailed string
	cause       error
}

func (e *DagExecutionError) Error() string {
	if e.FirstFailed == "" {
		return "dag execution failed"
	}
	if e.cause != nil {
		return fmt.Sprintf("dag execution failed: task %q: %v", e.FirstFailed, e.cause)
	}
	return fmt.Sprintf("dag execution failed: task %q", e.FirstFailed)
}

// Unwrap returns the first failed task's cause.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (e *DagExecutionError) Unwrap() error { return e.cause }

// ErrDepNotAvailable is returned by Get[T] when the requested dependency is
// absent from Deps (it failed, was skipped, or is not an inline dep).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
var ErrDepNotAvailable = errors.New("dag: dependency result not available")

// ErrDepTypeMismatch is returned by Get[T] when the stored dependency value
// is not assignable to T (defends serialization edges).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
var ErrDepTypeMismatch = errors.New("dag: dependency result type mismatch")
