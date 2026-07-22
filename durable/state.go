package durable

import (
	"crypto/md5" //nolint:gosec // identifier encoding, not cryptography
	"encoding/hex"
)

// operationStatus mirrors the OperationStatus values of the Lambda durable
// execution API.
type operationStatus string

// Operation statuses, as returned by the Lambda durable execution API.
const (
	statusStarted   operationStatus = "STARTED"
	statusPending   operationStatus = "PENDING"
	statusReady     operationStatus = "READY"
	statusSucceeded operationStatus = "SUCCEEDED"
	statusFailed    operationStatus = "FAILED"
	statusCancelled operationStatus = "CANCELLED"
	statusTimedOut  operationStatus = "TIMED_OUT"
	statusStopped   operationStatus = "STOPPED"
)

// terminal reports whether the status is a settled outcome that replay can
// return without re-executing the operation.
func (s operationStatus) terminal() bool {
	switch s {
	case statusSucceeded, statusFailed, statusCancelled, statusTimedOut, statusStopped:
		return true
	default:
		return false
	}
}

// operation is the checkpointed record of one durable operation, as
// returned by the Lambda durable execution API. Fields are added as the
// engine consumes them.
type operation struct {
	id       string
	status   operationStatus
	opType   string // wire Type (e.g. "STEP", "BATCH", "CONTEXT")
	subType  string // wire SubType (e.g. "Step", "WaitForCondition")
	name     string // caller-supplied operation name
	step     *stepDetails
	invoke   *invokeDetails
	childCtx *contextDetails
	callback *callbackDetails
}

// callbackDetails is the callback-specific portion of a checkpointed
// operation.
type callbackDetails struct {
	// callbackID is the identifier the external system uses to submit
	// a result via SendDurableExecutionCallback{Success,Failure,Heartbeat}.
	callbackID string

	// result is the serialized result payload. Set when the callback
	// succeeded.
	result string

	// errType and errMessage describe the callback failure. Set when the
	// callback failed or timed out.
	errType    string
	errMessage string
}

// invokeDetails is the chained-invoke portion of a checkpointed operation.
type invokeDetails struct {
	// result is the serialized result of the invoked function. Set when
	// the invoke succeeded.
	result string

	// errType, errMessage, and errData describe the invoked function's
	// failure. Set when the invoke failed.
	errType    string
	errMessage string
	errData    string
}

// contextDetails is the child-context portion of a checkpointed operation.
type contextDetails struct {
	// result is the serialized result of the child-context function.
	// Set when the context succeeded.
	result string

	// replayChildren indicates that the child context's result exceeded
	// the checkpoint payload limit. On replay the child body must be
	// re-executed to reconstruct the result rather than deserializing
	// from the checkpoint.
	replayChildren bool

	// errType and errMessage describe the child-context failure. Set
	// when the context failed.
	errType    string
	errMessage string
}

// stepDetails is the step-specific portion of a checkpointed operation.
type stepDetails struct {
	// attempt is the number of attempts already made.
	attempt int

	// result is the serialized step result. Set when the step succeeded.
	result string

	// errType and errMessage describe the failure recorded by the last
	// attempt. Set when the step failed.
	errType    string
	errMessage string
}

// executionState holds the checkpointed operations for the current
// execution, keyed by wire operation ID. Wire IDs are 16-hex-char truncated
// MD5 hashes of the positional operation ID; get hashes internally so
// callers use positional IDs throughout.
type executionState struct {
	operations map[string]*operation
}

func newExecutionState(ops []*operation) *executionState {
	m := make(map[string]*operation, len(ops))
	for _, op := range ops {
		m[op.id] = op
	}
	return &executionState{operations: m}
}

// get returns the checkpointed operation for the positional ID, or nil if
// the operation has not been checkpointed.
func (s *executionState) get(positionalID string) *operation {
	return s.operations[hashID(positionalID)]
}

// hashID converts a positional operation ID to its wire form: the first 16
// hex characters of its MD5 digest. MD5 is an identifier encoding here, not
// a security mechanism; 16 hex characters carry 64 bits, so collisions are
// negligible at realistic operation counts per execution.
func hashID(positionalID string) string {
	sum := md5.Sum([]byte(positionalID))
	return hex.EncodeToString(sum[:])[:16]
}
