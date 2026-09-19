package durable

import (
	"crypto/md5" //nolint:gosec // identifier encoding, not cryptography
	"encoding/hex"
	"sync"
	"time"
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
	parentID string
	status   operationStatus
	opType   string // wire Type (e.g. "STEP", "BATCH", "CONTEXT")
	subType  string // wire SubType (e.g. "Step", "WaitForCondition")
	name     string // caller-supplied operation name

	// startTimestamp and endTimestamp are parsed from the wire payload's
	// ISO 8601 strings. Zero when absent from the payload.
	startTimestamp time.Time
	endTimestamp   time.Time

	step     *stepDetails
	invoke   *invokeDetails
	childCtx *contextDetails
	callback *callbackDetails
}

// setTimestamps records the operation's start and end times from a wire
// record's optional timestamps. A nil pointer means the field was absent
// and leaves the corresponding timestamp zero.
//
// Every path that builds an operation from wire data (the invocation
// payload and checkpoint API responses) calls this, so an operation
// carries the same timestamps whichever way it arrived.
func (op *operation) setTimestamps(start, end *time.Time) {
	if start != nil {
		op.startTimestamp = *start
	}
	if end != nil {
		op.endTimestamp = *end
	}
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

	// errType, errMessage, errData, and stackTrace describe the callback
	// failure. Set when the callback failed or timed out.
	errType    string
	errMessage string
	errData    string
	stackTrace []string
}

// record returns the recorded failure.
func (d *callbackDetails) record() errorRecord {
	return errorRecord{errType: d.errType, message: d.errMessage, data: d.errData, stackTrace: d.stackTrace}
}

// invokeDetails is the chained-invoke portion of a checkpointed operation.
type invokeDetails struct {
	// result is the serialized result of the invoked function. Set when
	// the invoke succeeded.
	result string

	// errType, errMessage, errData, and stackTrace describe the invoked
	// function's failure. Set when the invoke failed.
	errType    string
	errMessage string
	errData    string
	stackTrace []string
}

// record returns the recorded failure.
func (d *invokeDetails) record() errorRecord {
	return errorRecord{errType: d.errType, message: d.errMessage, data: d.errData, stackTrace: d.stackTrace}
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

	// errType, errMessage, errData, and stackTrace describe the
	// child-context failure. Set when the context failed.
	errType    string
	errMessage string
	errData    string
	stackTrace []string
}

// record returns the recorded failure.
func (d *contextDetails) record() errorRecord {
	return errorRecord{errType: d.errType, message: d.errMessage, data: d.errData, stackTrace: d.stackTrace}
}

// stepDetails is the step-specific portion of a checkpointed operation.
type stepDetails struct {
	// attempt is the number of attempts already made.
	attempt int

	// result is the serialized step result. Set when the step succeeded.
	result string

	// errType, errMessage, errData, and stackTrace describe the failure
	// recorded by the last attempt. Set when the step failed.
	errType    string
	errMessage string
	errData    string
	stackTrace []string
}

// record returns the recorded failure.
func (d *stepDetails) record() errorRecord {
	return errorRecord{errType: d.errType, message: d.errMessage, data: d.errData, stackTrace: d.stackTrace}
}

// executionState holds the checkpointed operations for the current
// execution, keyed by wire operation ID. Wire IDs are 16-hex-char truncated
// MD5 hashes of the positional operation ID; get hashes internally so
// callers use positional IDs throughout.
type executionState struct {
	// mu protects the operations map from concurrent read/write access.
	// Readers (get, getByWireID, len, range) take RLock; writers (merge)
	// take Lock.
	mu         sync.RWMutex
	operations map[string]*operation

	// updated holds the wire IDs of the operations whose status changed
	// between the previous invocation and this one, as the invocation
	// payload reports them. It is written once, before the handler runs,
	// and only read afterwards, so it needs no lock. An operation in this
	// set settled outside the SDK's own invocations, so the invocation
	// that observes its terminal checkpoint reports the end as live, not
	// replayed.
	updated map[string]struct{}
}

func newExecutionState(ops []*operation) *executionState {
	m := make(map[string]*operation, len(ops))
	for _, op := range ops {
		m[op.id] = op
	}
	return &executionState{operations: m}
}

// setUpdatedOperationIDs records the wire IDs of the operations the
// invocation payload reports as changed since the previous invocation.
// Called once, before any operation runs.
func (s *executionState) setUpdatedOperationIDs(wireIDs []string) {
	if len(wireIDs) == 0 {
		s.updated = nil
		return
	}
	s.updated = make(map[string]struct{}, len(wireIDs))
	for _, id := range wireIDs {
		s.updated[id] = struct{}{}
	}
}

// updatedSinceLastInvocation reports whether the operation with the
// positional ID changed between the previous invocation and this one.
func (s *executionState) updatedSinceLastInvocation(positionalID string) bool {
	_, ok := s.updated[hashID(positionalID)]
	return ok
}

// get returns the checkpointed operation for the positional ID, or nil if
// the operation has not been checkpointed.
func (s *executionState) get(positionalID string) *operation {
	s.mu.RLock()
	op := s.operations[hashID(positionalID)]
	s.mu.RUnlock()
	return op
}

// getByWireID returns the checkpointed operation for an already-hashed wire
// operation ID, or nil if unknown. Used for IDs the service supplies in wire
// form (e.g. UpdatedOperationIds), which must not be hashed again.
func (s *executionState) getByWireID(wireID string) *operation {
	s.mu.RLock()
	op := s.operations[wireID]
	s.mu.RUnlock()
	return op
}

// merge inserts or replaces operations in the map. Callers that hold
// checkpointer.mu must call merge (not assign to the map directly) so that
// the lock order checkpointer.mu → executionState.mu is respected.
func (s *executionState) merge(ops []*operation) {
	s.mu.Lock()
	for _, op := range ops {
		s.operations[op.id] = op
	}
	s.mu.Unlock()
}

// numOperations returns the number of checkpointed operations.
func (s *executionState) numOperations() int {
	s.mu.RLock()
	n := len(s.operations)
	s.mu.RUnlock()
	return n
}

// rangeOperations calls fn for each operation in the map. If fn returns
// false, iteration stops.
func (s *executionState) rangeOperations(fn func(*operation) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, op := range s.operations {
		if !fn(op) {
			return
		}
	}
}

// executionOperation returns the root EXECUTION operation, or nil when the
// state holds none. The operation is found by its type, not by its
// position: the order of operations in the invocation payload is not
// part of the contract, so the first entry is not assumed to be the
// execution operation.
func (s *executionState) executionOperation() *operation {
	var found *operation
	s.rangeOperations(func(op *operation) bool {
		if op.opType == string(OperationTypeExecution) {
			found = op
			return false
		}
		return true
	})
	return found
}

// hashID converts a positional operation ID to its wire form: the first 16
// hex characters of its MD5 digest. MD5 is an identifier encoding here, not
// a security mechanism; 16 hex characters carry 64 bits, so collisions are
// negligible at realistic operation counts per execution.
func hashID(positionalID string) string {
	sum := md5.Sum([]byte(positionalID))
	return hex.EncodeToString(sum[:])[:16]
}

// operationResult returns the serialized result string for a terminal
// operation, or empty if unavailable. Used to populate OperationHookInfo.Result.
func (op *operation) operationResult() string {
	if op == nil {
		return ""
	}
	switch {
	case op.step != nil:
		return op.step.result
	case op.invoke != nil:
		return op.invoke.result
	case op.childCtx != nil:
		return op.childCtx.result
	case op.callback != nil:
		return op.callback.result
	default:
		return ""
	}
}

// operationError returns the error for a failed operation, or nil if not
// applicable. Used to populate OperationHookInfo.Error.
func (op *operation) operationError() error {
	if op == nil {
		return nil
	}
	switch {
	case op.step != nil && op.step.errType != "":
		return op.step.record().standIn(nil)
	case op.invoke != nil && op.invoke.errType != "":
		return op.invoke.record().standIn(nil)
	case op.childCtx != nil && op.childCtx.errType != "":
		return op.childCtx.record().standIn(nil)
	case op.callback != nil && op.callback.errType != "":
		return op.callback.record().standIn(nil)
	default:
		return nil
	}
}
