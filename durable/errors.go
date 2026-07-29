package durable

import (
	"errors"
	"fmt"
)

// Verify interface compliance for all error types.
var (
	_ error = (*StepError)(nil)
	_ error = (*StepInterruptedError)(nil)
	_ error = (*InvokeError)(nil)
	_ error = (*CallbackError)(nil)
	_ error = (*ChildContextError)(nil)
	_ error = (*WaitForConditionError)(nil)
	_ error = (*CombinatorError)(nil)
	_ error = (*OperationError)(nil)
	_ error = (*NonDeterministicReplayError)(nil)
	_ error = (*ResultTooLargeError)(nil)
	_ error = (*SerdesError)(nil)
)

// Sentinel errors for matching with [errors.Is]. These indicate terminal
// operation statuses assigned externally (TIMED_OUT, STOPPED, CANCELLED).

// ErrCallbackTimedOut indicates that a callback's timeout elapsed before an
// external system submitted a result.
var ErrCallbackTimedOut = errors.New("durable: callback timed out")

// ErrInvokeTimedOut indicates that an invoked function's execution timed
// out before producing a result.
var ErrInvokeTimedOut = errors.New("durable: invoke timed out")

// ErrExecutionStopped indicates that a durable execution was explicitly
// stopped via the StopDurableExecution API.
var ErrExecutionStopped = errors.New("durable: execution stopped")

// ErrExecutionCancelled indicates that a durable execution was cancelled.
var ErrExecutionCancelled = errors.New("durable: execution cancelled")

// OperationStatus represents the terminal status of a durable operation.
// It is used in [InvokeError] to indicate why an invoked function failed.
type OperationStatus string

// Terminal operation statuses.
const (
	// OperationStatusFailed indicates the operation's function returned an error.
	OperationStatusFailed OperationStatus = "FAILED"

	// OperationStatusTimedOut indicates the operation exceeded its timeout.
	OperationStatusTimedOut OperationStatus = "TIMED_OUT"

	// OperationStatusStopped indicates the execution was explicitly stopped.
	OperationStatusStopped OperationStatus = "STOPPED"

	// OperationStatusCancelled indicates the execution was cancelled.
	OperationStatusCancelled OperationStatus = "CANCELLED"
)

// OperationError is a common base that callers can match with a single
// [errors.As] call to determine that any durable operation produced the
// error, without needing to check each concrete type individually.
//
// Every typed operation error ([StepError], [InvokeError], [CallbackError],
// [ChildContextError], [WaitForConditionError], [CombinatorError],
// [NonDeterministicReplayError], [ResultTooLargeError]) is matchable via:
//
//	var opErr *durable.OperationError
//	if errors.As(err, &opErr) {
//	    log.Printf("operation %q failed: %v", opErr.Name, opErr.Err)
//	}
//
// OperationError does not alter the existing error types' public fields,
// Error() formats, or Unwrap chains. It is reachable via the As method
// that each typed error implements.
type OperationError struct {
	// Name is the operation's name as passed by the caller.
	Name string

	// Err is the underlying cause, if any.
	Err error
}

func (e *OperationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("durable: operation %q failed: %v", e.Name, e.Err)
	}
	return fmt.Sprintf("durable: operation %q failed", e.Name)
}

func (e *OperationError) Unwrap() error { return e.Err }

// StepError indicates that a step failed after exhausting its retry
// strategy. It wraps the error returned by the final attempt.
type StepError struct {
	// Name is the step's name, or the empty string for unnamed steps.
	Name string

	// Attempts is the number of times the step body executed.
	Attempts int

	// Err is the error from the final attempt.
	Err error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("durable: step %q failed after %d attempts: %v", e.Name, e.Attempts, e.Err)
}

func (e *StepError) Unwrap() error { return e.Err }

// As supports [errors.As] matching against [*OperationError].
func (e *StepError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name, Err: e.Err}
		return true
	}
	return false
}

// StepInterruptedError indicates that a step with at-most-once-per-retry
// semantics was interrupted before recording an outcome, and was not
// re-executed. It is passed to the step's retry strategy as the failed
// attempt's error.
type StepInterruptedError struct {
	// Name is the step's name, or the empty string for unnamed steps.
	Name string
}

func (e *StepInterruptedError) Error() string {
	return fmt.Sprintf("durable: step %q interrupted before completing an attempt", e.Name)
}

// As supports [errors.As] matching against [*OperationError].
func (e *StepInterruptedError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name}
		return true
	}
	return false
}

// InvokeError indicates that an invoked function failed or its execution
// timed out.
type InvokeError struct {
	// Name is the invoke operation's name.
	Name string

	// FunctionID is the function name or ARN that was invoked.
	FunctionID string

	// Status is the terminal operation status that caused the failure
	// (Failed, TimedOut, Stopped, or Cancelled). It enables callers to
	// distinguish the reason for failure without sentinel error matching.
	Status OperationStatus

	// Err is the underlying failure.
	Err error
}

func (e *InvokeError) Error() string {
	return fmt.Sprintf("durable: invoke %q of %q failed: %v", e.Name, e.FunctionID, e.Err)
}

func (e *InvokeError) Unwrap() error { return e.Err }

// As supports [errors.As] matching against [*OperationError].
func (e *InvokeError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name, Err: e.Err}
		return true
	}
	return false
}

// CallbackError indicates that a callback failed. It is the base error for
// all callback-related failures:
//
//   - External system reported failure → Err wraps a [replayedError]
//   - Callback timed out → errors.Is(err, [ErrCallbackTimedOut]) is true
//   - Submitter function failed → Err wraps the submitter's error
type CallbackError struct {
	// Name is the callback operation's name.
	Name string

	// CallbackID is the identifier that was issued to the external system.
	CallbackID string

	// Err is the underlying failure.
	Err error
}

func (e *CallbackError) Error() string {
	return fmt.Sprintf("durable: callback %q failed: %v", e.Name, e.Err)
}

func (e *CallbackError) Unwrap() error { return e.Err }

// As supports [errors.As] matching against [*OperationError].
func (e *CallbackError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name, Err: e.Err}
		return true
	}
	return false
}

// ChildContextError indicates that a child-context function failed.
type ChildContextError struct {
	// Name is the child context's name.
	Name string

	// Err is the error returned by the child-context function.
	Err error
}

func (e *ChildContextError) Error() string {
	return fmt.Sprintf("durable: child context %q failed: %v", e.Name, e.Err)
}

func (e *ChildContextError) Unwrap() error { return e.Err }

// As supports [errors.As] matching against [*OperationError].
func (e *ChildContextError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name, Err: e.Err}
		return true
	}
	return false
}

// WaitForConditionError indicates that a wait-for-condition operation
// failed: either the check function returned an error or the wait strategy
// decided to stop with an error.
type WaitForConditionError struct {
	// Name is the operation's name.
	Name string

	// Attempts is the number of times the check function was called.
	Attempts int

	// Err is the underlying failure from the check function or wait
	// strategy.
	Err error
}

func (e *WaitForConditionError) Error() string {
	return fmt.Sprintf("durable: wait-for-condition %q failed after %d attempts: %v", e.Name, e.Attempts, e.Err)
}

func (e *WaitForConditionError) Unwrap() error { return e.Err }

// As supports [errors.As] matching against [*OperationError].
func (e *WaitForConditionError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name, Err: e.Err}
		return true
	}
	return false
}

// CombinatorError indicates that a future combinator failed. For [Any],
// this wraps all individual future errors when no future succeeded.
type CombinatorError struct {
	// Name is the combinator operation's name.
	Name string

	// Errors contains the individual future errors.
	Errors []error
}

func (e *CombinatorError) Error() string {
	return fmt.Sprintf("durable: combinator %q: all futures failed (%d errors)", e.Name, len(e.Errors))
}

// Unwrap returns the individual errors for use with [errors.Is] and
// [errors.As].
func (e *CombinatorError) Unwrap() []error { return e.Errors }

// As supports [errors.As] matching against [*OperationError].
func (e *CombinatorError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name}
		return true
	}
	return false
}

// SerdesError indicates that a serialization or deserialization operation
// failed. It wraps the underlying serdes failure with context about which
// operation and direction (marshal/unmarshal) triggered it.
type SerdesError struct {
	// Operation is the name of the durable operation whose serdes failed.
	Operation string

	// Direction is "marshal" or "unmarshal", indicating whether
	// serialization or deserialization failed.
	Direction string

	// Err is the underlying serdes failure.
	Err error
}

func (e *SerdesError) Error() string {
	return fmt.Sprintf("durable: serdes %s failed for operation %q: %v", e.Direction, e.Operation, e.Err)
}

func (e *SerdesError) Unwrap() error { return e.Err }

// NonDeterministicReplayError indicates that a checkpointed operation's
// type does not match what the current code expects at the same position.
// This means the handler code changed between deployments in a way that
// breaks replay determinism.
type NonDeterministicReplayError struct {
	// Name is the operation name the current code passed.
	Name string

	// StepID is the positional ID where the mismatch was detected.
	StepID string

	// ExpectedType is the operation type the current code expects.
	ExpectedType string

	// ExpectedSubType is the sub-type the current code expects (may be
	// empty for operations that don't distinguish by sub-type).
	ExpectedSubType string

	// ExpectedName is the operation name the current code expects (may be
	// empty for unnamed operations).
	ExpectedName string

	// ActualType is the checkpointed operation's type.
	ActualType string

	// ActualSubType is the checkpointed operation's sub-type.
	ActualSubType string

	// ActualName is the checkpointed operation's name.
	ActualName string
}

func (e *NonDeterministicReplayError) Error() string {
	expected := e.ExpectedType
	if e.ExpectedSubType != "" {
		expected += "/" + e.ExpectedSubType
	}
	actual := e.ActualType
	if e.ActualSubType != "" {
		actual += "/" + e.ActualSubType
	}
	return fmt.Sprintf(
		"durable: non-deterministic replay at step %q (name %q): "+
			"expected %s but found %s — the handler code changed between deployments",
		e.StepID, e.Name, expected, actual,
	)
}

// As supports [errors.As] matching against [*OperationError].
func (e *NonDeterministicReplayError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name}
		return true
	}
	return false
}

// resultSizeLimitBytes is the checkpoint batch payload limit (750KB).
// A single operation result exceeding this cannot fit in any checkpoint
// batch and is rejected. This check applies only to
// paths where the SDK serializes a user-produced value directly into a
// checkpoint payload (Step results, WaitForCondition results). It does NOT
// apply to child-context results (which use ReplayChildren offload) or
// externally-produced values (callbacks, invokes).
const resultSizeLimitBytes = 750 * 1024

// ResultTooLargeError indicates that a single operation's serialized result
// exceeds the checkpoint payload limit. The caller should restructure the
// operation to return a reference (e.g., an S3 key) instead of the full
// payload, or supply a custom [Serdes] that offloads to external storage.
type ResultTooLargeError struct {
	// Name is the operation's name.
	Name string

	// SizeBytes is the serialized result's size.
	SizeBytes int

	// LimitBytes is the threshold that was exceeded.
	LimitBytes int
}

func (e *ResultTooLargeError) Error() string {
	return fmt.Sprintf(
		"durable: operation %q result is %d bytes, exceeding the %d-byte checkpoint limit — "+
			"return a reference instead of the full payload, or use a custom Serdes that offloads to external storage",
		e.Name, e.SizeBytes, e.LimitBytes,
	)
}

// As supports [errors.As] matching against [*OperationError].
func (e *ResultTooLargeError) As(target interface{}) bool {
	if t, ok := target.(**OperationError); ok {
		*t = &OperationError{Name: e.Name}
		return true
	}
	return false
}
