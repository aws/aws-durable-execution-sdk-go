package durable

import (
	"errors"
	"fmt"
	"strings"
)

// Verify interface compliance for the DAG error types.
var (
	_ error = (*DagValidationError)(nil)
	_ error = (*DagInvalidTaskNameError)(nil)
	_ error = (*DagDuplicateTaskError)(nil)
	_ error = (*DagInvalidDependencyError)(nil)
	_ error = (*DagCyclicDependencyError)(nil)
	_ error = (*DagInvalidConfigError)(nil)
	_ error = (*DagInvalidTriggerRuleError)(nil)
	_ error = (*DagInapplicableOptionError)(nil)
	_ error = (*DagExecutionError)(nil)
	_ error = (*DagError)(nil)
	_ error = (*DagTaskFailedError)(nil)
	_ error = (*DagPredicateError)(nil)
	_ error = (*DagRegistrationError)(nil)
)

// DagError indicates that a DAG scope-level operation failed. It is the DAG
// analogue of [ChildContextError] (the DAG scope is materialized as a
// CONTEXT operation), and is matchable via [errors.As] against
// [*OperationError].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagError struct {
	// Name is the DAG's name.
	Name string
	// Err is the underlying cause.
	Err error
}

func (e *DagError) Error() string {
	return fmt.Sprintf("durable: dag %q failed: %v", e.Name, e.Err)
}

func (e *DagError) Unwrap() error { return e.Err }

// As supports [errors.As] matching against [*OperationError].
func (e *DagError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name, Err: e.Err}
		return true
	}
	return false
}

// DagTaskFailedError indicates that a single task within a DAG failed. It
// is the DAG analogue of [BatchItemFailedError], wrapping the task's error
// with its name and registration index, and is matchable via [errors.As]
// against [*OperationError].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagTaskFailedError struct {
	// Name is the failed task's name.
	Name string
	// TaskID is the failed task's stable identifier.
	TaskID string
	// Err is the underlying task error.
	Err error
}

func (e *DagTaskFailedError) Error() string {
	return fmt.Sprintf("durable: dag task %q failed: %v", e.Name, e.Err)
}

func (e *DagTaskFailedError) Unwrap() error { return e.Err }

// As supports [errors.As] matching against [*OperationError].
func (e *DagTaskFailedError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name, Err: e.Err}
		return true
	}
	return false
}

// DagPredicateError indicates that a task's runIf predicate panicked. A
// runIf is specified as a synchronous, deterministic, pure predicate over
// resolved upstream results, so a panic there is a defect in deterministic
// code, not a business outcome. It therefore ABORTS the DAG rather than
// being reinterpreted as a task failure: recording it as a failure would
// silently drive every downstream ALL_FAILED / ANY_FAILED / ALL_DONE
// compensation path off a defect in the scheduler's own decision-making. The
// offending task is given no terminal state, no further tasks start, and
// [Dag] returns this error so the DAG container checkpoints a failure.
//
// The recovered panic value is wrapped as the cause (with the stack
// preserved); when the panic value was itself an error it is wrapped with
// %w, so [errors.Is] / [errors.As] reach the original.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagPredicateError struct {
	// Name is the offending task's name (the task whose runIf panicked).
	Name string
	// Err is the recovered panic value formatted as an error, with the
	// stack trace preserved.
	Err error
}

func (e *DagPredicateError) Error() string {
	return fmt.Sprintf("durable: dag task %q runIf predicate panicked: %v", e.Name, e.Err)
}

// Unwrap exposes the wrapped panic cause for errors.Is/errors.As traversal.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (e *DagPredicateError) Unwrap() error { return e.Err }

// DagRegistrationError indicates that the register callback passed to
// [Dag] panicked while building the task graph. Registration runs
// synchronously, before any task is scheduled, and is expected to be pure
// graph-construction code (calling DagStep, DagInvoke, etc. on the
// [DagBuilder]); a panic there is a defect in that construction code, not a
// business outcome, so it ABORTS the DAG the same way a panicking runIf
// does ([DagPredicateError]) rather than being reinterpreted as a task
// failure — there is no task graph yet for a failure to attach to, and no
// tasks have started, so nothing needs draining. [Dag] returns this error
// directly; the DAG container checkpoints a failure instead of the panic
// reaching the Lambda runtime and crashing the invocation.
//
// The recovered panic value is wrapped as the cause (with the stack
// preserved); when the panic value was itself an error it is wrapped with
// %w, so [errors.Is] / [errors.As] reach the original.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagRegistrationError struct {
	// Name is the DAG's name.
	Name string
	// Err is the recovered panic value formatted as an error, with the
	// stack trace preserved.
	Err error
}

func (e *DagRegistrationError) Error() string {
	return fmt.Sprintf("durable: dag %q register callback panicked: %v", e.Name, e.Err)
}

// Unwrap exposes the wrapped panic cause for errors.Is/errors.As traversal.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (e *DagRegistrationError) Unwrap() error { return e.Err }

// DagValidationError aggregates all registration/validation problems
// detected before any task is scheduled.
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

// DagInvalidConfigError reports an invalid DAG/task configuration.
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

// DagInapplicableOptionError reports a functional [DagOption] applied to a
// task registration whose target operation does not honor it.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagInapplicableOptionError struct{ Task, Option, Op string }

func (e *DagInapplicableOptionError) Error() string {
	return fmt.Sprintf("task %q (%s): option %s does not apply to this operation", e.Task, e.Op, e.Option)
}

// DagExecutionError is returned by [DagResult.ThrowIfError] when at least
// one task failed (or a custom predicate completed the DAG with a failure
// outcome). It wraps the first failed task's cause.
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

// As supports [errors.As] matching against [*OperationError].
func (e *DagExecutionError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.FirstFailed, Err: e.cause}
		return true
	}
	return false
}

// ErrDepNotAvailable is returned by [Get] when the requested dependency is
// absent from [Deps] (it failed, was skipped, or is not an inline dep).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
var ErrDepNotAvailable = errors.New("dag: dependency result not available")

// ErrDepTypeMismatch is returned by [Get] when the stored dependency value
// is not assignable to T.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
var ErrDepTypeMismatch = errors.New("dag: dependency result type mismatch")
