package durable

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Verify interface compliance for all error types.
var (
	_ error = (*StepError)(nil)
	_ error = (*StepInterruptedError)(nil)
	_ error = (*InvokeError)(nil)
	_ error = (*CallbackError)(nil)
	_ error = (*CallbackExternalError)(nil)
	_ error = (*CallbackTimeoutError)(nil)
	_ error = (*CallbackSubmitterError)(nil)
	_ error = (*ChildContextError)(nil)
	_ error = (*WaitForConditionError)(nil)
	_ error = (*CombinatorError)(nil)
	_ error = (*BatchCompletionError)(nil)
	_ error = (*OperationError)(nil)
	_ error = (*NonDeterministicReplayError)(nil)
	_ error = (*ResultTooLargeError)(nil)
	_ error = (*SerdesError)(nil)
)

// wireErrorType returns the ErrorType string recorded for err. Every path
// that serializes an error — checkpoint FAIL and RETRY updates, batch item
// results, and the FAILED invocation response — uses this one function, so
// the same Go error always produces the same wire name.
//
// The rule has two parts:
//
//  1. The chain is walked from the outermost error inward. The first SDK
//     error type found gives the name, via [sdkWireErrorType]. So a
//     [ChildContextError] wrapping a [StepError] is reported as
//     "ChildContextError", and a [StepError] wrapped by [fmt.Errorf] is
//     reported as "StepError".
//  2. If the chain holds no SDK error type, the outermost error's Go type
//     name is the wire name. Unnamed types (from [errors.New],
//     [fmt.Errorf], and [errors.Join]) are reported as "Error". The
//     wrapper returned by [WithErrorData] is transparent: it never
//     contributes a name.
func wireErrorType(err error) string {
	if sdkErr := outermostSDKError(err); sdkErr != nil {
		name, _ := sdkWireErrorType(sdkErr)
		return name
	}
	err = unwrapErrorData(err)
	if re, ok := err.(*replayedError); ok {
		return re.errType
	}
	return userErrorTypeName(err)
}

// unwrapErrorData removes the transparent metadata wrappers from the head
// of err's chain: the [WithErrorData] wrapper and the FLAT-mode batch item
// trace wrapper. Each carries metadata only; neither contributes a type or
// a message.
func unwrapErrorData(err error) error {
	for {
		switch w := err.(type) {
		case *errorWithData:
			err = w.err
		case *flatItemTraceError:
			err = w.err
		default:
			return err
		}
	}
}

// sdkWireErrorType maps an SDK error type to its wire ErrorType. It is the
// single source of these names; no other code derives them. The wire names
// are shared with the other Durable Execution SDKs, so consumers can match
// on one name regardless of the SDK that recorded the failure. The mapping
// is keyed on the concrete type, not on the type's name, so renaming a Go
// type cannot change the wire contract.
//
// The second result is false when err is not an SDK error type.
func sdkWireErrorType(err error) (string, bool) {
	switch err.(type) {
	case *StepError:
		return "StepError", true
	case *StepInterruptedError:
		return "StepInterruptedError", true
	case *InvokeError:
		return "InvokeError", true
	case *CallbackError:
		return "CallbackError", true
	case *CallbackExternalError:
		return "CallbackExternalError", true
	case *CallbackTimeoutError:
		return "CallbackTimeoutError", true
	case *CallbackSubmitterError:
		return "CallbackSubmitterError", true
	case *ChildContextError:
		return "ChildContextError", true
	case *WaitForConditionError:
		return "WaitForConditionError", true
	case *CombinatorError:
		return "PromiseCombinatorError", true
	case *BatchCompletionError:
		return "BatchCompletionError", true
	case *OperationError:
		return "OperationError", true
	case *SerdesError:
		return "SerdesError", true
	case *NonDeterministicReplayError:
		return "NonDeterministicReplayError", true
	case *ResultTooLargeError:
		return "ResultTooLargeError", true
	case *CheckpointError:
		return "CheckpointError", true
	}
	return "", false
}

// outermostSDKError returns the first SDK error type in err's chain,
// walking from the outermost error inward through Unwrap. Errors that
// unwrap to several causes are searched in cause order. It returns nil
// when the chain holds no SDK error type.
func outermostSDKError(err error) error {
	for err != nil {
		if _, ok := sdkWireErrorType(err); ok {
			return err
		}
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, cause := range u.Unwrap() {
				if found := outermostSDKError(cause); found != nil {
					return found
				}
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}

// userErrorTypeName derives the wire ErrorType for an error that is not an
// SDK error type: the error's concrete Go type name, so retry strategies
// keyed on error identity see a stable name. Unnamed error types (such as
// those from [errors.New], [fmt.Errorf], and [errors.Join]) map to "Error".
func userErrorTypeName(err error) string {
	t := reflect.TypeOf(err)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return "Error"
	}
	name := t.Name()
	if name == "" || name == "errorString" || name == "wrapError" || name == "joinError" {
		return "Error"
	}
	return name
}

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

// OperationStatus is the status of a durable operation. It covers the
// in-progress statuses (STARTED, PENDING, READY), the successful terminal
// status (SUCCEEDED), and the failure terminal statuses (FAILED, TIMED_OUT,
// STOPPED, CANCELLED).
//
// It appears in two places. [Operation.Status] carries it in checkpoint
// records exchanged with the service. [InvokeError.Status] carries a failure
// terminal status to indicate why an invoked function failed.
type OperationStatus string

// Failure terminal operation statuses, reported by [InvokeError]. The
// in-progress statuses and SUCCEEDED are declared in client.go alongside
// [Operation].
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

// OperationError is the common shape of every failure a durable operation
// reports. Callers match it with a single [errors.As] call to determine
// that any durable operation produced the error, without checking each
// concrete type:
//
//	var opErr *durable.OperationError
//	if errors.As(err, &opErr) {
//	    log.Printf("operation %q failed with %s: %s", opErr.Name, opErr.ErrorType, opErr.Message)
//	}
//
// Every typed operation error ([StepError], [InvokeError], [CallbackError]
// and its subtypes, [ChildContextError], [WaitForConditionError],
// [CombinatorError], [StepInterruptedError], [BatchCompletionError],
// [NonDeterministicReplayError], [ResultTooLargeError]) is matchable this
// way. The typed errors that wrap a recorded failure expose the same
// fields directly.
//
// # The wrapped cause is a stand-in
//
// A failure is recorded in the checkpoint as four fields: the escaping
// error's type name (ErrorType), its message, optional ErrorData, and an
// optional stack trace. Err is rebuilt from those fields on both the first
// invocation and on replay; it never holds the original Go error value.
// So [errors.As] against a caller's own error type is false on every
// invocation, not just on replay. Match on ErrorType instead:
//
//	var stepErr *durable.StepError
//	if errors.As(err, &stepErr) && stepErr.ErrorType == "PaymentDeclinedError" {
//	    // handle the decline
//	}
//
// ErrorType is the escaping error's Go type name, as derived when the
// failure was recorded: the concrete type's name, or "Error" for unnamed
// types such as those from [errors.New] and [fmt.Errorf]. The SDK's own
// error types use fixed names shared with the other Durable Execution
// SDKs, so a [StepError] escaping a child context is recorded as
// "StepError".
//
// Where the SDK defines a sentinel ([ErrCallbackTimedOut],
// [ErrInvokeTimedOut], [ErrExecutionStopped], [ErrExecutionCancelled]),
// the stand-in unwraps to it, so [errors.Is] against the sentinel is true
// on both the first invocation and on replay.
type OperationError struct {
	// Name is the operation's name as passed by the caller.
	Name string

	// ErrorType is the wire ErrorType of the escaping error, for example
	// "PaymentDeclinedError". It is empty for errors that record no cause.
	ErrorType string

	// Message is the escaping error's recorded message.
	Message string

	// ErrorData is the optional structured payload attached with
	// [WithErrorData]. It round-trips through checkpoints unchanged.
	ErrorData string

	// StackTrace holds the stack trace lines recorded for the failure, when
	// the recorder captured one. It is nil otherwise.
	StackTrace []string

	// Err is the stand-in for the recorded failure, built from ErrorType
	// and Message. It is the same value on the first invocation and on
	// replay. It is nil for errors that record no cause.
	Err error
}

func (e *OperationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("durable: operation %q failed: %v", e.Name, e.Err)
	}
	return fmt.Sprintf("durable: operation %q failed", e.Name)
}

func (e *OperationError) Unwrap() error { return e.Err }

func (e *OperationError) operationError() *OperationError { return e }

// errorRecord is a recorded failure in Go form: the wire ErrorObject fields
// the SDK reads back from a checkpoint or derives from a live error.
type errorRecord struct {
	errType    string
	message    string
	data       string
	stackTrace []string
}

// recordOf derives the record the SDK writes for a live error: the wire
// ErrorType, the error's message, any [WithErrorData] payload found in
// its chain, and the stack trace already recorded for the outermost SDK
// error in the chain, if any. A stand-in yields the record it was built
// from, so recording an operation error's cause a second time does not
// prefix the message with the type name again.
func recordOf(err error) errorRecord {
	rec := errorRecord{errType: wireErrorType(err), message: err.Error(), data: errorDataOf(err)}
	if re, ok := unwrapErrorData(err).(*replayedError); ok {
		rec.message = re.message
	}
	if oe, ok := outermostSDKError(err).(interface{ operationError() *OperationError }); ok {
		rec.stackTrace = oe.operationError().StackTrace
	}
	return rec
}

// withTrace returns the record carrying trace as its stack trace. A trace
// the record already holds wins: it was recorded closer to the failure's
// origin, by the operation that failed first.
func (r errorRecord) withTrace(trace []string) errorRecord {
	if len(r.stackTrace) == 0 {
		r.stackTrace = trace
	}
	return r
}

// standIn builds the leaf stand-in error for a record. sentinel, when
// non-nil, is reachable through Unwrap so [errors.Is] matches it.
func (r errorRecord) standIn(sentinel error) *replayedError {
	return &replayedError{errType: r.errType, message: r.message, sentinel: sentinel}
}

// cause builds the Err value of a typed operation error from a record. A
// record whose ErrorType names an SDK error type is rebuilt as that type by
// [reconstructSDKError], so [errors.As] reaches an SDK error that escaped a
// child context on the first invocation and on replay alike. Any other
// record yields the leaf stand-in.
//
// The record does not carry the inner operation's name. The one exception
// is a combinator: [Any], [Race], and the other combinators run in a child
// context named after themselves, so a "PromiseCombinatorError" rebuilt as
// the cause of a child context takes that context's name, passed as name.
func (r errorRecord) cause(name string, sentinel error) error {
	if _, ok := sdkErrorsByWireType[r.errType]; ok {
		op := OperationError{ErrorType: r.errType, Message: r.message, ErrorData: r.data, StackTrace: r.stackTrace}
		if r.errType == "PromiseCombinatorError" {
			op.Name = name
		}
		return reconstructSDKError(r.errType, op, sentinel)
	}
	return r.standIn(sentinel)
}

// causeText formats a recorded cause for an Error() string as
// "<ErrorType>: <Message>". A record without a message falls back to err's
// own text, so hand-built errors that set only Err still print their cause.
func causeText(errType, message string, err error) string {
	if message == "" {
		if err != nil {
			return err.Error()
		}
		return ""
	}
	if errType == "" {
		return message
	}
	return errType + ": " + message
}

// maxErrorDataBytes bounds the ErrorData payload recorded with a failure.
// Longer payloads are truncated on a UTF-8 boundary when recorded, so a
// large payload can never make a checkpoint update exceed the service's
// request limit.
const maxErrorDataBytes = 256 * 1024

// WithErrorData returns an error that wraps err and carries data as the
// failure's ErrorData. When the error escapes a step, a child context, a
// wait-for-condition check, or the handler itself, the SDK records data in
// the checkpoint's ErrorData field alongside the error's type and message.
// The data is then readable as the ErrorData field of the [StepError],
// [ChildContextError], or other typed error the caller receives, and it
// is identical on the first invocation and on replay.
//
// The returned error reports err's message and unwraps to err, so
// [errors.Is] and [errors.As] see through it, and it does not change the
// ErrorType recorded for err. When several wrapped errors appear in one
// chain, the outermost data wins. Wrapping a nil error returns nil.
//
// Data longer than 256 KiB is truncated on a UTF-8 boundary when it is
// recorded; the truncated form is what the typed error carries.
func WithErrorData(err error, data string) error {
	if err == nil {
		return nil
	}
	return &errorWithData{err: err, data: data}
}

// errorWithData is the wrapper returned by [WithErrorData].
type errorWithData struct {
	err  error
	data string
}

func (e *errorWithData) Error() string { return e.err.Error() }

func (e *errorWithData) Unwrap() error { return e.err }

// errorDataOf returns the ErrorData carried in err's chain: the outermost
// [WithErrorData] payload, or the ErrorData of the outermost typed
// operation error, whichever comes first. It returns "" when the chain
// carries none. The result is truncated to [maxErrorDataBytes].
func errorDataOf(err error) string {
	for err != nil {
		switch e := err.(type) {
		case *errorWithData:
			if e.data != "" {
				return truncateUTF8(e.data, maxErrorDataBytes)
			}
		case interface{ operationError() *OperationError }:
			if op := e.operationError(); op.ErrorData != "" {
				return truncateUTF8(op.ErrorData, maxErrorDataBytes)
			}
		}
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, cause := range u.Unwrap() {
				if data := errorDataOf(cause); data != "" {
					return data
				}
			}
			return ""
		default:
			return ""
		}
	}
	return ""
}

// asOperationError implements the As method every typed error exposes:
// it assigns op to target when target is **OperationError.
func asOperationError(target any, op *OperationError) bool {
	if t, ok := target.(**OperationError); ok {
		*t = op
		return true
	}
	return false
}

// StepError indicates that a step failed after exhausting its retry
// strategy.
//
// Err is a stand-in rebuilt from ErrorType and Message; it is the same on
// the first invocation and on replay and never holds the step body's
// original error value. Match on ErrorType rather than with [errors.As]
// against the body's error type. See [OperationError].
type StepError struct {
	// Name is the step's name, or the empty string for unnamed steps.
	Name string

	// Attempts is the number of times the step body executed.
	Attempts int

	// ErrorType is the wire ErrorType of the final attempt's error.
	ErrorType string

	// Message is the final attempt's recorded error message.
	Message string

	// ErrorData is the payload attached with [WithErrorData], if any.
	ErrorData string

	// StackTrace holds recorded stack trace lines, when captured.
	StackTrace []string

	// Err is the stand-in for the final attempt's error.
	Err error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("durable: step %q failed after %d attempts: %s", e.Name, e.Attempts, causeText(e.ErrorType, e.Message, e.Err))
}

func (e *StepError) Unwrap() error { return e.Err }

func (e *StepError) operationError() *OperationError {
	return &OperationError{Name: e.Name, ErrorType: e.ErrorType, Message: e.Message, ErrorData: e.ErrorData, StackTrace: e.StackTrace, Err: e.Err}
}

// As supports [errors.As] matching against [*OperationError].
func (e *StepError) As(target any) bool { return asOperationError(target, e.operationError()) }

// StepInterruptedError indicates that a step with at-most-once-per-retry
// semantics was interrupted before recording an outcome, and was not
// re-executed. It is passed to the step's retry strategy as the failed
// attempt's error. It records no cause, so the [OperationError] it reaches
// has an empty ErrorType and a nil Err.
type StepInterruptedError struct {
	// Name is the step's name, or the empty string for unnamed steps.
	Name string
}

func (e *StepInterruptedError) Error() string {
	return fmt.Sprintf("durable: step %q interrupted before completing an attempt", e.Name)
}

func (e *StepInterruptedError) operationError() *OperationError {
	return &OperationError{Name: e.Name, Message: e.Error()}
}

// As supports [errors.As] matching against [*OperationError].
func (e *StepInterruptedError) As(target any) bool {
	return asOperationError(target, e.operationError())
}

// InvokeError indicates that an invoked function failed or its execution
// timed out.
//
// Err is a stand-in rebuilt from ErrorType and Message, the same on the
// first invocation and on replay. It unwraps to [ErrInvokeTimedOut],
// [ErrExecutionStopped], or [ErrExecutionCancelled] when Status is the
// matching terminal status. Match on ErrorType or Status. See
// [OperationError].
type InvokeError struct {
	// Name is the invoke operation's name.
	Name string

	// FunctionID is the function name or ARN that was invoked.
	FunctionID string

	// Status is the terminal operation status that caused the failure
	// (Failed, TimedOut, Stopped, or Cancelled). It enables callers to
	// distinguish the reason for failure without sentinel error matching.
	Status OperationStatus

	// ErrorType is the wire ErrorType the invoked function recorded.
	ErrorType string

	// Message is the invoked function's recorded error message.
	Message string

	// ErrorData is the invoked function's recorded ErrorData, if any.
	ErrorData string

	// StackTrace holds recorded stack trace lines, when captured.
	StackTrace []string

	// Err is the stand-in for the invoked function's failure.
	Err error
}

func (e *InvokeError) Error() string {
	return fmt.Sprintf("durable: invoke %q of %q failed: %s", e.Name, e.FunctionID, causeText(e.ErrorType, e.Message, e.Err))
}

func (e *InvokeError) Unwrap() error { return e.Err }

func (e *InvokeError) operationError() *OperationError {
	return &OperationError{Name: e.Name, ErrorType: e.ErrorType, Message: e.Message, ErrorData: e.ErrorData, StackTrace: e.StackTrace, Err: e.Err}
}

// As supports [errors.As] matching against [*OperationError].
func (e *InvokeError) As(target any) bool { return asOperationError(target, e.operationError()) }

// CallbackError indicates that a callback failed. It is the base error for
// all callback-related failures; [errors.As] against *CallbackError matches
// every subtype:
//
//   - [CallbackExternalError]: the external system reported failure.
//   - [CallbackTimeoutError]: the callback or its heartbeat timed out.
//   - [CallbackSubmitterError]: the [WaitForCallback] submitter step failed.
//
// Err is a stand-in rebuilt from ErrorType and Message, the same on the
// first invocation and on replay. For a timeout it unwraps to
// [ErrCallbackTimedOut]. Match on the subtype or on ErrorType. See
// [OperationError].
type CallbackError struct {
	// Name is the callback operation's name.
	Name string

	// CallbackID is the identifier that was issued to the external system.
	CallbackID string

	// ErrorType is the wire ErrorType recorded for the failure: the type
	// the external system reported, or the SDK's own name for the failure
	// mode when the record carries none.
	ErrorType string

	// Message is the recorded failure message.
	Message string

	// ErrorData is the recorded ErrorData, if any.
	ErrorData string

	// StackTrace holds recorded stack trace lines, when captured.
	StackTrace []string

	// Err is the stand-in for the recorded failure.
	Err error
}

func (e *CallbackError) Error() string {
	return fmt.Sprintf("durable: callback %q failed: %s", e.Name, causeText(e.ErrorType, e.Message, e.Err))
}

func (e *CallbackError) Unwrap() error { return e.Err }

func (e *CallbackError) operationError() *OperationError {
	return &OperationError{Name: e.Name, ErrorType: e.ErrorType, Message: e.Message, ErrorData: e.ErrorData, StackTrace: e.StackTrace, Err: e.Err}
}

// As supports [errors.As] matching against [*OperationError].
func (e *CallbackError) As(target any) bool {
	return asOperationError(target, e.operationError())
}

// asCallbackError implements the As method of the CallbackError subtypes:
// it assigns the embedded CallbackError when target is **CallbackError,
// and otherwise defers to the OperationError match.
func asCallbackError(target any, e *CallbackError) bool {
	if t, ok := target.(**CallbackError); ok {
		*t = e
		return true
	}
	return asOperationError(target, e.operationError())
}

// CallbackExternalError indicates that the external system completed the
// callback with a failure (SendDurableExecutionCallbackFailure). ErrorType
// and Message are the values the external system supplied. The wrapped
// cause is a reconstructed stand-in; match on ErrorType. See
// [CallbackError].
type CallbackExternalError struct {
	CallbackError
}

func (e *CallbackExternalError) Error() string {
	return fmt.Sprintf("durable: callback %q failed externally: %s", e.Name, causeText(e.ErrorType, e.Message, e.Err))
}

// As supports [errors.As] matching against [*CallbackError] and
// [*OperationError].
func (e *CallbackExternalError) As(target any) bool { return asCallbackError(target, &e.CallbackError) }

// CallbackTimeoutError indicates that the callback's timeout, or its
// heartbeat timeout, elapsed before the external system submitted a result.
// Err unwraps to [ErrCallbackTimedOut] on both the first invocation and on
// replay. The wrapped cause is a reconstructed stand-in; match on the type,
// on Heartbeat, or on ErrorType. See [CallbackError].
type CallbackTimeoutError struct {
	CallbackError

	// Heartbeat is true when the heartbeat timeout elapsed, and false when
	// the overall callback timeout elapsed. It is derived from the timeout
	// message the service recorded.
	Heartbeat bool
}

func (e *CallbackTimeoutError) Error() string {
	if e.Heartbeat {
		return fmt.Sprintf("durable: callback %q heartbeat timed out: %s", e.Name, causeText(e.ErrorType, e.Message, e.Err))
	}
	return fmt.Sprintf("durable: callback %q timed out: %s", e.Name, causeText(e.ErrorType, e.Message, e.Err))
}

// As supports [errors.As] matching against [*CallbackError] and
// [*OperationError].
func (e *CallbackTimeoutError) As(target any) bool { return asCallbackError(target, &e.CallbackError) }

// CallbackSubmitterError indicates that the submitter step of a
// [WaitForCallback] failed after exhausting its retry strategy. ErrorType
// and Message describe the submitter's final error. The wrapped cause is a
// reconstructed stand-in; match on ErrorType. See [CallbackError].
type CallbackSubmitterError struct {
	CallbackError
}

func (e *CallbackSubmitterError) Error() string {
	return fmt.Sprintf("durable: callback %q submitter failed: %s", e.Name, causeText(e.ErrorType, e.Message, e.Err))
}

// As supports [errors.As] matching against [*CallbackError] and
// [*OperationError].
func (e *CallbackSubmitterError) As(target any) bool {
	return asCallbackError(target, &e.CallbackError)
}

// Default messages for callback failures whose record carries no message,
// shared with the other Durable Execution SDKs.
const (
	callbackTimedOutMessage = "Callback timed out"
	callbackFailedMessage   = "Callback failed"
)

// Older wire ErrorType names for a callback timeout. Checkpoints written
// before the callback subtypes existed record a timed-out callback under
// one of these names, with the heartbeat kind named by the type rather
// than by the message. Both are read as a [CallbackTimeoutError].
const (
	legacyCallbackTimeoutType   = "Callback.Timeout"
	legacyCallbackHeartbeatType = "Callback.Heartbeat"
)

// isCallbackTimeoutType reports whether a wire ErrorType names a callback
// timeout, in the current or an older form.
func isCallbackTimeoutType(errType string) bool {
	return errType == "CallbackTimeoutError" || errType == legacyCallbackTimeoutType || errType == legacyCallbackHeartbeatType
}

// isHeartbeatTimeoutRecord reports whether a recorded callback timeout is
// a heartbeat timeout. The service records the same TIMED_OUT status for
// both timeout kinds and names the heartbeat in the message; an older
// record names it in the ErrorType instead.
func isHeartbeatTimeoutRecord(rec errorRecord) bool {
	return rec.errType == legacyCallbackHeartbeatType || strings.Contains(strings.ToLower(rec.message), "heartbeat")
}

// newCallbackExternalError builds the error for a callback the external
// system completed with a failure.
func newCallbackExternalError(name, callbackID string, rec errorRecord) *CallbackExternalError {
	if rec.errType == "" {
		rec.errType = "CallbackExternalError"
	}
	if rec.message == "" {
		rec.message = callbackFailedMessage
	}
	return &CallbackExternalError{CallbackError: CallbackError{
		Name: name, CallbackID: callbackID,
		ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace,
		Err: rec.standIn(nil),
	}}
}

// newCallbackTimeoutError builds the error for a callback whose timeout or
// heartbeat timeout elapsed. The stand-in unwraps to [ErrCallbackTimedOut].
// ErrorType keeps the recorded name, including an older form, so the
// record reads back as it was written.
func newCallbackTimeoutError(name, callbackID string, rec errorRecord) *CallbackTimeoutError {
	if rec.errType == "" {
		rec.errType = "CallbackTimeoutError"
	}
	if rec.message == "" {
		rec.message = callbackTimedOutMessage
	}
	return &CallbackTimeoutError{
		CallbackError: CallbackError{
			Name: name, CallbackID: callbackID,
			ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace,
			Err: rec.standIn(ErrCallbackTimedOut),
		},
		Heartbeat: isHeartbeatTimeoutRecord(rec),
	}
}

// newCallbackSubmitterError builds the error for a [WaitForCallback] whose
// submitter step failed.
func newCallbackSubmitterError(name, callbackID string, rec errorRecord) *CallbackSubmitterError {
	return &CallbackSubmitterError{CallbackError: CallbackError{
		Name: name, CallbackID: callbackID,
		ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace,
		Err: rec.standIn(nil),
	}}
}

// sdkErrorsByWireType lists the wire names that [reconstructSDKError]
// rebuilds as SDK error types. It is derived from [sdkWireErrorType] at
// package initialization so the two never disagree on a name, and it adds
// the older callback timeout names.
var sdkErrorsByWireType = func() map[string]struct{} {
	m := map[string]struct{}{}
	for _, err := range []error{
		&StepError{}, &StepInterruptedError{}, &InvokeError{},
		&CallbackError{}, &CallbackExternalError{}, &CallbackTimeoutError{}, &CallbackSubmitterError{},
		&ChildContextError{}, &WaitForConditionError{}, &CombinatorError{}, &SerdesError{},
		&OperationError{}, &BatchCompletionError{}, &NonDeterministicReplayError{}, &ResultTooLargeError{},
	} {
		name, _ := sdkWireErrorType(err)
		m[name] = struct{}{}
	}
	m[legacyCallbackTimeoutType] = struct{}{}
	m[legacyCallbackHeartbeatType] = struct{}{}
	return m
}()

// reconstructSDKError rebuilds the SDK error type named by a wire ErrorType
// from an OperationError view, on the first invocation and on replay alike.
// It is used for the cause of a child-context failure whose record names an
// SDK type, and for a rejected [Settled] value.
//
// The result carries op's fields. Its Err is built the way the live
// constructor builds it. When op.ErrorType names a different SDK type than
// wireType (a child context whose step failed records "StepError"), Err is
// that inner type rebuilt by [errorRecord.cause], so [errors.As] against
// the inner type matches the same way it does on the first invocation.
// When op.ErrorType equals wireType, the record named only the outer type,
// so Err is a leaf stand-in; this also ends the recursion, because the
// rebuilt inner type's own record names itself. sentinel, when non-nil, is
// reachable through Err. Fields outside [OperationError] (Attempts,
// FunctionID, CallbackID, Status, Direction, and the detail fields of
// [NonDeterministicReplayError] and [ResultTooLargeError]) are zero, with
// two exceptions: a callback timeout derives Heartbeat from the record and
// unwraps to [ErrCallbackTimedOut], and a [BatchCompletionError] recovers
// its Reason from the message. The types whose Error() text is composed
// from detail fields keep the recorded message as their Error() text
// instead.
//
// An unknown name yields the leaf stand-in itself: a value whose Error() is
// "<ErrorType>: <Message>" and which matches no SDK type.
func reconstructSDKError(wireType string, op OperationError, sentinel error) error {
	rec := errorRecord{errType: op.ErrorType, message: op.Message, data: op.ErrorData, stackTrace: op.StackTrace}
	if rec.errType == "" {
		rec.errType = wireType
	}
	leaf := rec.standIn(sentinel)
	// inner is the Err of a type whose live constructor uses
	// [errorRecord.cause]: the nested SDK type when the record names one,
	// the leaf otherwise.
	var inner error = leaf
	if rec.errType != wireType {
		inner = rec.cause(op.Name, sentinel)
	}
	switch wireType {
	case "OperationError":
		if op.ErrorType == "" && op.Message == "" {
			// The record named an OperationError that carried no cause.
			return &OperationError{Name: op.Name, ErrorData: rec.data, StackTrace: rec.stackTrace}
		}
		return &OperationError{Name: op.Name, ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace, Err: inner}
	case "StepError":
		return &StepError{Name: op.Name, ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace, Err: inner}
	case "StepInterruptedError":
		return &StepInterruptedError{Name: op.Name}
	case "InvokeError":
		return &InvokeError{Name: op.Name, ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace, Err: inner}
	case "CallbackError":
		return &CallbackError{Name: op.Name, ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace, Err: leaf}
	case "CallbackExternalError":
		return newCallbackExternalError(op.Name, "", rec)
	case "CallbackTimeoutError", legacyCallbackTimeoutType, legacyCallbackHeartbeatType:
		return newCallbackTimeoutError(op.Name, "", rec)
	case "CallbackSubmitterError":
		return newCallbackSubmitterError(op.Name, "", rec)
	case "ChildContextError":
		return &ChildContextError{Name: op.Name, ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace, Err: inner}
	case "WaitForConditionError":
		return &WaitForConditionError{Name: op.Name, ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace, Err: inner}
	case "PromiseCombinatorError":
		return &CombinatorError{Name: op.Name, Errors: []error{inner}}
	case "BatchCompletionError":
		return &BatchCompletionError{Reason: completionReasonOf(rec.message), recordedMessage: rec.message}
	case "SerdesError":
		return &SerdesError{Operation: op.Name, Err: leaf}
	case "NonDeterministicReplayError":
		return &NonDeterministicReplayError{Name: op.Name, recordedMessage: rec.message}
	case "ResultTooLargeError":
		return &ResultTooLargeError{Name: op.Name, recordedMessage: rec.message}
	}
	return leaf
}

// completionReasonOf recovers the [CompletionReason] named in a recorded
// [BatchCompletionError] message. It returns zero when the message names
// no known reason.
func completionReasonOf(message string) CompletionReason {
	for _, r := range []CompletionReason{CompletionAllCompleted, CompletionMinSuccessfulReached, CompletionFailureToleranceExceeded} {
		if message == (&BatchCompletionError{Reason: r}).Error() {
			return r
		}
	}
	return 0
}

// ChildContextError indicates that a child-context function failed.
//
// ErrorType names the error that escaped the child body: "StepError" when
// a step inside the child failed, "ChildContextError" for a nested child,
// or the body's own error type. Err is a stand-in rebuilt from the record,
// the same on the first invocation and on replay. When ErrorType names an
// SDK error type, Err is that type rebuilt with its [OperationError]
// fields, so [errors.As] against the inner type (for example [*StepError])
// succeeds; the inner type's own detail fields, such as
// [StepError.Attempts], are zero. When ErrorType names a caller's type,
// Err is a leaf stand-in that [errors.As] against that type does not
// match; match on ErrorType. ErrorData attached anywhere in the child's
// failure chain propagates through every nesting level. See
// [OperationError].
type ChildContextError struct {
	// Name is the child context's name.
	Name string

	// ErrorType is the wire ErrorType of the error that escaped the child.
	ErrorType string

	// Message is the escaping error's recorded message.
	Message string

	// ErrorData is the payload attached with [WithErrorData], if any.
	ErrorData string

	// StackTrace holds recorded stack trace lines, when captured.
	StackTrace []string

	// Err is the stand-in for the error that escaped the child.
	Err error
}

func (e *ChildContextError) Error() string {
	return fmt.Sprintf("durable: child context %q failed: %s", e.Name, causeText(e.ErrorType, e.Message, e.Err))
}

func (e *ChildContextError) Unwrap() error { return e.Err }

func (e *ChildContextError) operationError() *OperationError {
	return &OperationError{Name: e.Name, ErrorType: e.ErrorType, Message: e.Message, ErrorData: e.ErrorData, StackTrace: e.StackTrace, Err: e.Err}
}

// As supports [errors.As] matching against [*OperationError].
func (e *ChildContextError) As(target any) bool {
	return asOperationError(target, e.operationError())
}

// newChildContextError builds the error for a child context whose body
// failed, from the failure record.
func newChildContextError(name string, rec errorRecord) *ChildContextError {
	return &ChildContextError{
		Name: name, ErrorType: rec.errType, Message: rec.message, ErrorData: rec.data, StackTrace: rec.stackTrace,
		Err: rec.cause(name, nil),
	}
}

// WaitForConditionError indicates that a wait-for-condition operation
// failed: either the check function returned an error or the wait strategy
// decided to stop with an error.
//
// Err is a stand-in rebuilt from ErrorType and Message, the same on the
// first invocation and on replay. Match on ErrorType. See [OperationError].
type WaitForConditionError struct {
	// Name is the operation's name.
	Name string

	// Attempts is the number of times the check function was called.
	Attempts int

	// ErrorType is the wire ErrorType of the check or strategy error.
	ErrorType string

	// Message is the recorded error message.
	Message string

	// ErrorData is the payload attached with [WithErrorData], if any.
	ErrorData string

	// StackTrace holds recorded stack trace lines, when captured.
	StackTrace []string

	// Err is the stand-in for the failure from the check function or wait
	// strategy.
	Err error
}

func (e *WaitForConditionError) Error() string {
	return fmt.Sprintf("durable: wait-for-condition %q failed after %d attempts: %s", e.Name, e.Attempts, causeText(e.ErrorType, e.Message, e.Err))
}

func (e *WaitForConditionError) Unwrap() error { return e.Err }

func (e *WaitForConditionError) operationError() *OperationError {
	return &OperationError{Name: e.Name, ErrorType: e.ErrorType, Message: e.Message, ErrorData: e.ErrorData, StackTrace: e.StackTrace, Err: e.Err}
}

// As supports [errors.As] matching against [*OperationError].
func (e *WaitForConditionError) As(target any) bool {
	return asOperationError(target, e.operationError())
}

// CombinatorError indicates that a future combinator failed. For [Any],
// this wraps all individual future errors when no future succeeded.
//
// The combinator runs in a child context, so the error a caller receives
// is a [ChildContextError] whose cause is a CombinatorError rebuilt from
// the recorded failure: Errors then holds one stand-in carrying the
// recorded message, on the first invocation and on replay alike. Match on
// the type and on [ChildContextError.ErrorType]. See [OperationError].
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

func (e *CombinatorError) operationError() *OperationError {
	return &OperationError{Name: e.Name, Message: e.Error()}
}

// As supports [errors.As] matching against [*OperationError].
func (e *CombinatorError) As(target any) bool {
	return asOperationError(target, e.operationError())
}

// BatchCompletionError is returned by [BatchResult.Err] when a batch
// completed as failed at the batch level without any item carrying its own
// error — for example, a [BatchResult] reconstructed from public state
// (where item errors do not survive serialization) whose status indicates
// failure.
//
// A value rebuilt from a checkpoint record (for example the Err of a
// rejected [Settled]) recovers Reason from the recorded message; an
// unrecognized message leaves Reason zero and is kept as the Error() text.
type BatchCompletionError struct {
	// Reason is the completion reason of the failed batch.
	Reason CompletionReason

	// recordedMessage is the Error() text of a value rebuilt from a
	// checkpoint record. It is empty for a value the batch produced.
	recordedMessage string
}

func (e *BatchCompletionError) Error() string {
	if e.recordedMessage != "" {
		return e.recordedMessage
	}
	return fmt.Sprintf("durable: batch failed: %s", e.Reason)
}

func (e *BatchCompletionError) operationError() *OperationError {
	return &OperationError{Message: e.Error()}
}

// As supports [errors.As] matching against [*OperationError].
func (e *BatchCompletionError) As(target any) bool {
	return asOperationError(target, e.operationError())
}

// SerdesError indicates that a serialization or deserialization operation
// failed. It wraps the underlying serdes failure with context about which
// operation and direction (marshal/unmarshal) triggered it.
//
// A SerdesError returned directly by an operation holds the serdes's own
// error as Err. A SerdesError reached through a typed operation error (for
// example a [StepError] whose ErrorType is "SerdesError") is rebuilt from
// the recorded failure, and its Err is a stand-in carrying the recorded
// message; match on the type and the operation error's ErrorType.
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

// Serdes failure directions recorded in [SerdesError.Direction].
const (
	serdesDirectionMarshal   = "marshal"
	serdesDirectionUnmarshal = "unmarshal"
)

// newSerdesError wraps a failure at a configurable [Serdes] boundary,
// carrying the operation name and the direction that failed. Every
// Marshal/Unmarshal call on an operation's serdes reports failure through
// this wrapper; internal envelope serialization does not.
func newSerdesError(operation, direction string, cause error) *SerdesError {
	return &SerdesError{Operation: operation, Direction: direction, Err: cause}
}

// NonDeterministicReplayError indicates that a checkpointed operation's
// type does not match what the current code expects at the same position.
// This means the handler code changed between deployments in a way that
// breaks replay determinism.
//
// A value rebuilt from a checkpoint record (for example the Err of a
// rejected [Settled]) carries Name and the recorded Error() text; the
// detail fields are zero.
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

	// recordedMessage is the Error() text of a value rebuilt from a
	// checkpoint record. It is empty for a value replay produced.
	recordedMessage string
}

func (e *NonDeterministicReplayError) Error() string {
	if e.recordedMessage != "" {
		return e.recordedMessage
	}
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

func (e *NonDeterministicReplayError) operationError() *OperationError {
	return &OperationError{Name: e.Name, Message: e.Error()}
}

// As supports [errors.As] matching against [*OperationError].
func (e *NonDeterministicReplayError) As(target any) bool {
	return asOperationError(target, e.operationError())
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
//
// A value rebuilt from a checkpoint record (for example the Err of a
// rejected [Settled]) carries Name and the recorded Error() text; SizeBytes
// and LimitBytes are zero.
type ResultTooLargeError struct {
	// Name is the operation's name.
	Name string

	// SizeBytes is the serialized result's size.
	SizeBytes int

	// LimitBytes is the threshold that was exceeded.
	LimitBytes int

	// recordedMessage is the Error() text of a value rebuilt from a
	// checkpoint record. It is empty for a value the size check produced.
	recordedMessage string
}

func (e *ResultTooLargeError) Error() string {
	if e.recordedMessage != "" {
		return e.recordedMessage
	}
	return fmt.Sprintf(
		"durable: operation %q result is %d bytes, exceeding the %d-byte checkpoint limit — "+
			"return a reference instead of the full payload, or use a custom Serdes that offloads to external storage",
		e.Name, e.SizeBytes, e.LimitBytes,
	)
}

func (e *ResultTooLargeError) operationError() *OperationError {
	return &OperationError{Name: e.Name, Message: e.Error()}
}

// As supports [errors.As] matching against [*OperationError].
func (e *ResultTooLargeError) As(target any) bool {
	return asOperationError(target, e.operationError())
}
