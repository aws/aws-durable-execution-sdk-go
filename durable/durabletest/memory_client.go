// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// strptr returns a pointer to the given string.
func strptr(s string) *string { return &s }

// boolptr returns a pointer to the given bool.
func boolptr(b bool) *bool { return &b }

// Compile-time check that memoryClient satisfies ExecutionClient.
var _ durable.ExecutionClient = (*memoryClient)(nil)

// memoryClient is an in-memory implementation of [durable.ExecutionClient]
// for local testing. It stores operations, applies checkpoint updates, and
// rotates checkpoint tokens per the backend contract.
//
// Thread-safety: all public methods are goroutine-safe.
type memoryClient struct {
	mu         sync.Mutex
	token      string
	tokenSeq   int
	operations map[string]*durable.Operation // keyed by operation ID
	opOrder    []string                      // insertion-order tracking

	// invokeTargets records, per CHAINED_INVOKE operation ID, the function
	// identifier and input payload the handler passed to the invoke. The
	// runner reads them to dispatch registered functions.
	invokeTargets map[string]invokeTarget

	// omitTokenIn counts the Checkpoint calls remaining until one returns
	// a response without a token. Zero means no omission is scheduled.
	omitTokenIn int

	// events is the execution's history event log in recording order.
	// eventSeq is the ID of the most recently recorded event.
	events   []types.Event
	eventSeq int32

	// executionStarted is set once the ExecutionStarted event is recorded.
	// executionEnded is set once a terminal execution event is recorded,
	// so a later invocation response does not record a second one.
	executionStarted bool
	executionEnded   bool

	// executionStart is the time the execution started: the clock when the
	// ExecutionStarted event was recorded. Every invocation payload carries
	// it as the execution operation's StartTimestamp, which the handler
	// reports from durable.ExecutionStartTime. Zero until the execution
	// starts.
	executionStart time.Time

	// clock is the client's virtual clock. It is set when the execution
	// starts and moves forward only when the client advances it: to report
	// a timed operation finished while the handler ran other work (see
	// advanceTimersLocked), or to force every pending timer
	// (completePendingTimers). Wall-clock time does not move it. Every
	// timestamp the client records comes from this clock.
	clock time.Time

	// changed holds, in order, the IDs of operations whose status changed
	// outside a checkpoint request (a callback resolved, a chained invoke
	// settled, a timer forced) and that no checkpoint response has
	// reported yet. The next checkpoint response reports them, so the
	// handler observes the change in the invocation that is running.
	changed []string

	// tokenWithheld is set when a response withheld the checkpoint token
	// during the current invocation. The invocation then ends PENDING
	// because the service asked it to, not because it awaits an
	// operation.
	tokenWithheld bool

	// startInvokes, when set, runs the registered targets of chained
	// invokes a checkpoint request starts. Checkpoint calls it without
	// holding mu, after it has stored the START updates, and reports in
	// its response every invoke the targets settled.
	startInvokes func([]openInvoke)

	// delivered holds, per operation ID, the record of the operation as the
	// current invocation last received it: in its payload or in a
	// checkpoint response. settled holds the same map as it stood when
	// the last successful invocation ended. The next invocation payload
	// lists, as updated, every operation whose record differs from
	// settled, as the service does: an operation whose state changed
	// since the last successful invocation. A failed invocation leaves
	// settled unchanged, so the next payload lists again what the failed
	// invocation received.
	delivered map[string]string
	settled   map[string]string

	// checkpointedEnd holds the outcome the handler checkpointed on the
	// execution operation, which it does when a result is too large to
	// return inline. The terminal event is not recorded at checkpoint
	// time: it is recorded from this outcome once the invocation response
	// arrives, after that invocation's InvocationCompleted event, so the
	// terminal event stays last as it is in a cloud history. Nil when no
	// such checkpoint has been made.
	checkpointedEnd *checkpointedExecutionEnd
}

// checkpointedExecutionEnd is the terminal outcome a checkpoint on the
// execution operation carries: the action (SUCCEED or FAIL) and the
// result payload or error that goes with it.
type checkpointedExecutionEnd struct {
	action durable.OperationAction
	result *string
	err    *durable.ErrorObject
}

// invokeTarget is what a chained invoke asked for: the function to run and
// the serialized input to run it with.
type invokeTarget struct {
	functionID string
	payload    string
}

func newMemoryClient() *memoryClient {
	m := &memoryClient{}
	m.mu.Lock()
	m.resetLocked()
	m.mu.Unlock()
	return m
}

// reset returns the client to the state of a newly created one: no
// operations, no invoke targets, no events, the initial token, and no
// scheduled token omission.
func (m *memoryClient) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetLocked()
}

// resetLocked initializes every field to its initial value. Caller must
// hold m.mu.
func (m *memoryClient) resetLocked() {
	m.token = "test-token-0"
	m.tokenSeq = 0
	m.operations = make(map[string]*durable.Operation)
	m.opOrder = nil
	m.invokeTargets = make(map[string]invokeTarget)
	m.omitTokenIn = 0
	m.events = nil
	m.eventSeq = 0
	m.executionStarted = false
	m.executionEnded = false
	m.executionStart = time.Time{}
	m.checkpointedEnd = nil
	m.clock = time.Now().UTC()
	m.changed = nil
	m.tokenWithheld = false
	m.delivered = make(map[string]string)
	m.settled = make(map[string]string)
}

// nowLocked returns the client's virtual clock. Caller must hold m.mu.
func (m *memoryClient) nowLocked() time.Time {
	return m.clock
}

// now returns the client's clock; see nowLocked.
func (m *memoryClient) now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nowLocked()
}

// executionStartTime returns the time the execution started; see
// executionStart.
func (m *memoryClient) executionStartTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.executionStart
}

// advanceToLocked moves the client's clock forward to t. A t that is not
// after the current time leaves the clock as it is. Caller must hold m.mu.
func (m *memoryClient) advanceToLocked(t time.Time) {
	if t.After(m.clock) {
		m.clock = t
	}
}

// markChangedLocked records that the operation id changed outside a
// checkpoint request, so the next checkpoint response reports it. Caller
// must hold m.mu.
func (m *memoryClient) markChangedLocked(id string) {
	for _, c := range m.changed {
		if c == id {
			return
		}
	}
	m.changed = append(m.changed, id)
}

// beginInvocation takes the state an invocation payload carries and clears
// the per-invocation state, under one lock. It returns every stored
// operation except the execution operation, and the current checkpoint
// token.
//
// The payload carries every operation, so a change that no checkpoint
// response reported is delivered with it, and the pending change list is
// cleared. The snapshot and the clear happen under the same lock. So a
// change made concurrently, for example a callback resolved by the test,
// is either in the snapshot or in the change list the next checkpoint
// response reports. It is never lost between the two.
func (m *memoryClient) beginInvocation() ([]operationSnapshot, string) {
	ops, token, _ := m.beginInvocationWithUpdates()
	return ops, token
}

// beginInvocationWithUpdates is beginInvocation that also returns the IDs
// of the operations the payload lists as updated: those whose record
// changed since the last successful invocation ended, in insertion order.
func (m *memoryClient) beginInvocationWithUpdates() ([]operationSnapshot, string, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ops := m.snapshotLocked()
	m.changed = nil
	m.tokenWithheld = false
	var updated []string
	m.delivered = make(map[string]string, len(ops))
	for _, op := range ops {
		record := snapshotRecord(op)
		if m.settled[op.ID] != record {
			updated = append(updated, op.ID)
		}
		m.delivered[op.ID] = record
	}
	return ops, m.token, updated
}

// endInvocation records how the current invocation ended. A successful
// invocation makes the records it received the base the next payload's
// updated operations are computed from.
func (m *memoryClient) endInvocation(successful bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !successful {
		return
	}
	m.settled = make(map[string]string, len(m.delivered))
	for id, record := range m.delivered {
		m.settled[id] = record
	}
}

// snapshotRecord returns the record of an operation as an invocation
// receives it, in a form two records compare by.
func snapshotRecord(op operationSnapshot) string {
	b, err := json.Marshal(apiOperationToWire(op))
	if err != nil {
		// The wire shape holds only strings, numbers, and booleans.
		panic(fmt.Sprintf("durabletest: encode operation record: %v", err))
	}
	return string(b)
}

// Checkpoint validates the operation updates against the service's limits,
// applies them, rotates the checkpoint token, and returns the operations
// whose records changed. Per the backend contract, the token rotates on
// every successful checkpoint response.
//
// A request with an update the service rejects returns the service's error
// and stores nothing.
//
// Like the service, the response also reports operations that changed
// since the last response without a request of the handler: a callback
// resolved, a chained invoke settled by a registered target, and a timed
// operation the client's clock has reached (see advanceTimersLocked).
func (m *memoryClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	if err := validateCheckpoint(in); err != nil {
		return durable.CheckpointOutput{}, err
	}

	m.mu.Lock()
	// Apply each update.
	// lastUpdate holds, per operation the request updates, the position
	// of its last update in the request.
	lastUpdate := make(map[string]int, len(in.Updates))
	var updated []durable.Operation
	var started []openInvoke
	for i, u := range in.Updates {
		op := m.applyUpdate(u)
		updated = append(updated, op)
		id := ptrStr(u.Id)
		lastUpdate[id] = i
		if u.Type == durable.OperationTypeChainedInvoke && u.Action == durable.OperationActionStart {
			started = append(started, openInvoke{id: id, name: ptrStr(u.Name), target: m.invokeTargets[id]})
		}
	}
	startInvokes := m.startInvokes
	m.mu.Unlock()

	// A registered target runs without the lock: a durable target is an
	// execution of its own, and settling its invoke takes the lock.
	if startInvokes != nil && len(started) > 0 {
		startInvokes(started)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Virtual time passes only where the handler spent time: during a poll,
	// which the SDK sends while a goroutine is blocked, or during the work
	// a request reports finished. A request that only starts work, such as
	// a STEP START, reports no time spent, so no timer fires on it. A timer
	// the request itself starts did not run while the work ran, so only
	// timers started by earlier requests are considered.
	if len(in.Updates) == 0 || reportsFinishedWork(in.Updates) {
		inRequest := make(map[string]bool, len(lastUpdate))
		for id := range lastUpdate {
			inRequest[id] = true
		}
		m.advanceTimersLocked(inRequest)
	}
	for _, id := range m.changed {
		if _, inRequest := lastUpdate[id]; inRequest {
			// The response already carries the record this request
			// produced; replace it with the current one.
			for i := range updated {
				if ptrStr(updated[i].Id) == id {
					updated[i] = deepCopyOperation(*m.operations[id])
				}
			}
			continue
		}
		if op := m.operations[id]; op != nil {
			updated = append(updated, deepCopyOperation(*op))
		}
	}
	m.changed = nil
	for _, op := range updated {
		if op.Type == durable.OperationTypeExecution {
			continue
		}
		m.delivered[ptrStr(op.Id)] = snapshotRecord(operationToSnapshot(op))
	}

	// Rotate token.
	m.tokenSeq++
	m.token = "test-token-" + strconv.Itoa(m.tokenSeq)

	out := durable.CheckpointOutput{
		CheckpointToken:   m.token,
		NewExecutionState: updated,
	}
	if m.omitTokenIn > 0 {
		m.omitTokenIn--
		if m.omitTokenIn == 0 {
			// The updates are applied and the internal token rotates
			// as usual; only the response withholds the token. The next
			// invocation starts from the rotated token.
			out.CheckpointToken = ""
			m.tokenWithheld = true
		}
	}
	return out, nil
}

// reportsFinishedWork reports whether updates record the end of work the
// handler ran: a STEP attempt that succeeded, failed, or will be retried
// (a step body or a condition check ran), or a CONTEXT that succeeded or
// failed (a child function ran).
func reportsFinishedWork(updates []durable.OperationUpdate) bool {
	for _, u := range updates {
		switch u.Type {
		case durable.OperationTypeStep:
			switch u.Action {
			case durable.OperationActionSucceed, durable.OperationActionFail, durable.OperationActionRetry:
				return true
			}
		case durable.OperationTypeContext:
			switch u.Action {
			case durable.OperationActionSucceed, durable.OperationActionFail:
				return true
			}
		}
	}
	return false
}

// advanceTimersLocked reports the timed operations the handler waits on
// whose time has come while the handler ran other work. A timed operation
// is a WAIT in STARTED, which ends at its scheduled end time, and a STEP in
// PENDING, a retry or condition check whose next attempt is due at its next
// attempt time. An operation in excluded is not considered: the current
// request started or changed it, so it did not wait while the work ran.
//
// When at least one timed operation remains, the client moves its clock to
// the earliest of their times. Every one whose time is at or before the
// new clock then changes: a WAIT becomes SUCCEEDED and a STEP becomes
// READY. The changed operations are added to m.changed, so the response
// reports them.
//
// Caller must hold m.mu.
func (m *memoryClient) advanceTimersLocked(excluded map[string]bool) {
	var earliest time.Time
	for _, id := range m.opOrder {
		if excluded[id] {
			continue
		}
		due, ok := timerDue(m.operations[id])
		if ok && (earliest.IsZero() || due.Before(earliest)) {
			earliest = due
		}
	}
	if earliest.IsZero() {
		return
	}
	m.advanceToLocked(earliest)
	now := m.nowLocked()
	for _, id := range m.opOrder {
		if excluded[id] {
			continue
		}
		op := m.operations[id]
		due, ok := timerDue(op)
		if !ok || due.After(now) {
			continue
		}
		m.fireTimerLocked(id, op)
	}
}

// timerDue returns the time at which a timed operation is due: the
// scheduled end of a STARTED wait, or the next attempt time of a PENDING
// step. ok is false for any other operation and for one that carries no
// time.
func timerDue(op *durable.Operation) (due time.Time, ok bool) {
	if op == nil {
		return time.Time{}, false
	}
	switch {
	case op.Type == durable.OperationTypeWait && op.Status == durable.OperationStatusStarted &&
		op.WaitDetails != nil && op.WaitDetails.ScheduledEndTimestamp != nil:
		return *op.WaitDetails.ScheduledEndTimestamp, true
	case op.Type == durable.OperationTypeStep && op.Status == durable.OperationStatusPending &&
		op.StepDetails != nil && op.StepDetails.NextAttemptTimestamp != nil:
		return *op.StepDetails.NextAttemptTimestamp, true
	}
	return time.Time{}, false
}

// fireTimerLocked completes the timed operation op stored under id: a WAIT
// in STARTED becomes SUCCEEDED and a STEP in PENDING becomes READY. It
// records the change in m.changed. Caller must hold m.mu.
func (m *memoryClient) fireTimerLocked(id string, op *durable.Operation) bool {
	switch {
	case op.Type == durable.OperationTypeStep && op.Status == durable.OperationStatusPending:
		// PENDING → READY: retry timer elapsed.
		updated := *op
		updated.Status = durable.OperationStatusReady
		m.operations[id] = &updated
	case op.Type == durable.OperationTypeWait && op.Status == durable.OperationStatusStarted:
		// STARTED → SUCCEEDED: wait elapsed.
		updated := *op
		updated.Status = durable.OperationStatusSucceeded
		now := m.stampTransition(&updated)
		m.operations[id] = &updated
		m.recordOperationEvent(&updated, types.EventTypeWaitSucceeded, nil, nil, now)
	default:
		return false
	}
	m.markChangedLocked(id)
	return true
}

// omitTokenOnCheckpoint schedules the n-th Checkpoint call from now
// (1-based) to return a response without a token. It replaces any earlier
// schedule that has not fired yet.
func (m *memoryClient) omitTokenOnCheckpoint(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.omitTokenIn = n
}

// GetExecutionState returns all stored operations in a single page. The
// local runner does not use pagination; this method exists to satisfy the
// interface for the initial loadState pagination loop. Each returned
// operation is a deep copy, independent of the memoryClient's internal
// state.
func (m *memoryClient) GetExecutionState(_ context.Context, _ durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ops := make([]durable.Operation, 0, len(m.operations))
	for _, id := range m.opOrder {
		if op, ok := m.operations[id]; ok {
			ops = append(ops, deepCopyOperation(*op))
		}
	}
	return durable.GetExecutionStateOutput{
		Operations: ops,
	}, nil
}

// allOperations returns a deep-copied snapshot of all stored operations in
// insertion order, excluding the execution operation. Each returned
// Operation is fully independent of the memoryClient's internal state,
// preventing data races if the caller reads details while a concurrent
// checkpoint mutates the store.
func (m *memoryClient) allOperations() []durable.Operation {
	m.mu.Lock()
	defer m.mu.Unlock()

	ops := make([]durable.Operation, 0, len(m.operations))
	for _, id := range m.opOrder {
		op, ok := m.operations[id]
		if !ok {
			continue
		}
		// Skip the execution operation (type EXECUTION).
		if op.Type == durable.OperationTypeExecution {
			continue
		}
		ops = append(ops, deepCopyOperation(*op))
	}
	return ops
}

// currentToken returns the current checkpoint token.
func (m *memoryClient) currentToken() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.token
}

// applyUpdate processes a single OperationUpdate, creating or updating an
// operation in the store. It applies the action→status transition and
// builds type-specific details.
//
// Caller must hold m.mu.
func (m *memoryClient) applyUpdate(u durable.OperationUpdate) durable.Operation {
	id := ptrStr(u.Id)

	existing := m.operations[id]
	op := durable.Operation{
		Id:       u.Id,
		Name:     u.Name,
		Type:     u.Type,
		SubType:  u.SubType,
		ParentId: u.ParentId,
		Status:   deriveStatus(u.Action),
	}
	if existing != nil {
		// Timestamps describe the operation's whole life, not one
		// update, so they carry over from the stored record.
		op.StartTimestamp = existing.StartTimestamp
		op.EndTimestamp = existing.EndTimestamp
	}
	now := m.stampTransition(&op)

	// Build type-specific details.
	switch u.Type {
	case durable.OperationTypeStep:
		op.StepDetails = buildStepDetails(u, existing, now)
	case durable.OperationTypeWait:
		op.WaitDetails = buildWaitDetails(u, existing, now)
	case durable.OperationTypeCallback:
		op.CallbackDetails = buildCallbackDetails(u, existing)
	case durable.OperationTypeChainedInvoke:
		op.ChainedInvokeDetails = buildChainedInvokeDetails(u)
		if u.Action == durable.OperationActionStart {
			target := invokeTarget{payload: ptrStr(u.Payload)}
			if u.ChainedInvokeOptions != nil {
				target.functionID = ptrStr(u.ChainedInvokeOptions.FunctionName)
			}
			m.invokeTargets[id] = target
		}
	case durable.OperationTypeContext:
		op.ContextDetails = buildContextDetails(u)
	case durable.OperationTypeExecution:
		// Execution operations carry no updatable details.
	default:
		// Unknown types (including BATCH on the wire) carry no
		// operation-specific details in the checkpoint response.
	}

	// Store with insertion-order tracking.
	if _, existed := m.operations[id]; !existed {
		m.opOrder = append(m.opOrder, id)
	}
	m.operations[id] = &op
	m.recordUpdateEvent(u, op, now)
	return op
}

// stampTransition records the time of the status transition op has just
// made, read from the client's clock, and returns that time, so the
// matching history event can carry the same timestamp. The first STARTED status sets StartTimestamp;
// a later STARTED (a step re-entered after a retry) leaves it as it is. A
// terminal status sets EndTimestamp. PENDING and READY are intermediate
// and change neither.
//
// Caller must hold m.mu.
func (m *memoryClient) stampTransition(op *durable.Operation) time.Time {
	now := m.nowLocked()
	switch op.Status {
	case durable.OperationStatusStarted:
		if op.StartTimestamp == nil {
			op.StartTimestamp = &now
		}
	case durable.OperationStatusSucceeded, durable.OperationStatusFailed,
		durable.OperationStatusCancelled, durable.OperationStatusTimedOut,
		durable.OperationStatusStopped:
		op.EndTimestamp = &now
	}
	return now
}

// deriveStatus maps an OperationAction to the resulting OperationStatus.
func deriveStatus(action durable.OperationAction) durable.OperationStatus {
	switch action {
	case durable.OperationActionStart:
		return durable.OperationStatusStarted
	case durable.OperationActionSucceed:
		return durable.OperationStatusSucceeded
	case durable.OperationActionFail:
		return durable.OperationStatusFailed
	case durable.OperationActionRetry:
		return durable.OperationStatusPending
	case durable.OperationActionCancel:
		return durable.OperationStatusCancelled
	default:
		return durable.OperationStatus(string(action))
	}
}

// buildStepDetails constructs StepDetails for a checkpoint update, carrying
// forward attempt count and applying the action semantics.
//
// The payload of every action is stored as the step's Result. A RETRY
// carries the intermediate state of a polling step (WaitForCondition), and
// the next attempt reads that state back from the operation log. Dropping
// it would restart every attempt from the initial state, so the step would
// never observe accumulated progress.
//
// A RETRY records the next attempt time: now plus the update's
// NextAttemptDelaySeconds.
func buildStepDetails(u durable.OperationUpdate, existing *durable.Operation, now time.Time) *durable.StepDetails {
	var attempt int32
	if existing != nil && existing.StepDetails != nil {
		attempt = existing.StepDetails.Attempt
	}

	sd := &durable.StepDetails{}
	switch u.Action {
	case durable.OperationActionStart:
		// START preserves existing attempt count (step re-entered after
		// crash or retry timer). Only increment on RETRY/FAIL.
		sd.Attempt = attempt
	case durable.OperationActionSucceed:
		sd.Attempt = attempt
	case durable.OperationActionFail:
		sd.Attempt = attempt + 1
	case durable.OperationActionRetry:
		sd.Attempt = attempt + 1
	default:
		sd.Attempt = attempt
	}
	if u.Payload != nil {
		sd.Result = u.Payload
	}
	if u.Error != nil {
		sd.Error = u.Error
	}
	if u.Action == durable.OperationActionRetry {
		var delay int32
		if u.StepOptions != nil && u.StepOptions.NextAttemptDelaySeconds != nil {
			delay = *u.StepOptions.NextAttemptDelaySeconds
		}
		next := now.Add(time.Duration(delay) * time.Second)
		sd.NextAttemptTimestamp = &next
	}
	return sd
}

// buildWaitDetails constructs WaitDetails for a wait checkpoint update. A
// START records the scheduled end time: now plus the update's WaitSeconds.
// Any other action keeps the details of the stored record.
func buildWaitDetails(u durable.OperationUpdate, existing *durable.Operation, now time.Time) *durable.WaitDetails {
	if u.Action != durable.OperationActionStart {
		if existing != nil {
			return existing.WaitDetails
		}
		return nil
	}
	var secs int32
	if u.WaitOptions != nil && u.WaitOptions.WaitSeconds != nil {
		secs = *u.WaitOptions.WaitSeconds
	}
	end := now.Add(time.Duration(secs) * time.Second)
	return &durable.WaitDetails{ScheduledEndTimestamp: &end}
}

// buildCallbackDetails constructs CallbackDetails, generating a callback ID
// on START and preserving it on subsequent updates.
func buildCallbackDetails(u durable.OperationUpdate, existing *durable.Operation) *durable.CallbackDetails {
	cd := &durable.CallbackDetails{}

	// Preserve or generate callback ID.
	if existing != nil && existing.CallbackDetails != nil && existing.CallbackDetails.CallbackId != nil {
		cd.CallbackId = existing.CallbackDetails.CallbackId
	} else {
		// On START, the backend generates a callback ID. Use the
		// operation's hashed ID as the callback ID — deterministic and
		// unique within one execution.
		cd.CallbackId = u.Id
	}

	// On non-START actions, apply result/error.
	if u.Action != durable.OperationActionStart {
		if u.Payload != nil {
			cd.Result = u.Payload
		}
		if u.Error != nil {
			cd.Error = u.Error
		}
	}
	return cd
}

// buildChainedInvokeDetails constructs ChainedInvokeDetails.
func buildChainedInvokeDetails(u durable.OperationUpdate) *durable.ChainedInvokeDetails {
	if u.Action == durable.OperationActionStart {
		return &durable.ChainedInvokeDetails{}
	}
	return &durable.ChainedInvokeDetails{
		Result: u.Payload,
		Error:  u.Error,
	}
}

// buildContextDetails constructs ContextDetails.
func buildContextDetails(u durable.OperationUpdate) *durable.ContextDetails {
	cd := &durable.ContextDetails{
		Result: u.Payload,
		Error:  u.Error,
	}
	if u.ContextOptions != nil && u.ContextOptions.ReplayChildren != nil && *u.ContextOptions.ReplayChildren {
		cd.ReplayChildren = boolptr(true)
	}
	return cd
}

// --- Status constants matching the wire protocol ---

const (
	statusSucceeded = "SUCCEEDED"
	statusFailed    = "FAILED"
	statusTimedOut  = "TIMED_OUT"
	statusStarted   = "STARTED"
	statusPending   = "PENDING"
	statusReady     = "READY"
)

// errTypeCallbackTimedOut is the ErrorType recorded on the CallbackTimedOut
// event when [LocalRunner.TimeoutCallback] times out a callback.
const errTypeCallbackTimedOut = "CallbackTimedOut"

// errTypeChainedInvokeTimedOut is the ErrorType recorded on the
// ChainedInvokeTimedOut event when [LocalRunner.TimeoutChainedInvoke] times
// out a chained invoke.
const errTypeChainedInvokeTimedOut = "ChainedInvokeTimedOut"

// operationResult is a value type representing the outcome to apply to an
// operation, used by callback and chained-invoke helpers.
type operationResult struct {
	status     string
	result     string
	errType    string
	errMsg     string
	errData    string
	stackTrace []string
}

// OpenCallback identifies a callback operation pending external resolution.
type OpenCallback struct {
	// CallbackID is the identifier to pass to [LocalRunner.SendCallbackSuccess]
	// or [LocalRunner.SendCallbackFailure].
	CallbackID string

	// Name is the caller-supplied operation name.
	Name string
}

// completePendingTimers transitions timer-blocked operations, whatever
// their scheduled time:
//   - STEP in PENDING → READY (retry timer elapsed)
//   - WAIT in STARTED → SUCCEEDED (wait duration elapsed)
//
// The client's clock moves forward to the latest time among them, so the
// recorded timestamps follow the scheduled times. Returns true if any
// operation was advanced.
func (m *memoryClient) completePendingTimers() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	var latest time.Time
	for _, id := range m.opOrder {
		if due, ok := timerDue(m.operations[id]); ok && due.After(latest) {
			latest = due
		}
	}
	if !latest.IsZero() {
		m.advanceToLocked(latest)
	}
	advanced := false
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if m.fireTimerLocked(id, op) {
			advanced = true
		}
	}
	return advanced
}

// hasPendingOperation reports whether any stored operation is pending. A
// pending operation is one whose status STARTED, PENDING, or READY can be
// changed by something other than the handler: the service, a timer, or an
// external actor. So the service has a reason to invoke the handler again.
// These are a WAIT, CALLBACK, or CHAINED_INVOKE in STARTED status, and a
// STEP in STARTED, PENDING, or READY status.
//
// A CONTEXT in STARTED status is not pending. Only the handler's own
// checkpoints change a context, so a STARTED context gives the service no
// reason to invoke the handler again. Race over no futures records exactly
// such a context and responds PENDING, and the service rejects that
// response. The execution operation is not pending either.
func (m *memoryClient) hasPendingOperation() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, op := range m.operations {
		switch op.Type {
		case durable.OperationTypeWait, durable.OperationTypeCallback, durable.OperationTypeChainedInvoke:
			if op.Status == durable.OperationStatusStarted {
				return true
			}
		case durable.OperationTypeStep:
			switch op.Status {
			case durable.OperationStatusStarted, durable.OperationStatusPending, durable.OperationStatusReady:
				return true
			}
		}
	}
	return false
}

// withheldToken reports whether a checkpoint response withheld the token
// during the current invocation.
func (m *memoryClient) withheldToken() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tokenWithheld
}

// completeCallback applies a result to a callback operation identified by
// its callback ID.
func (m *memoryClient) completeCallback(callbackID string, result operationResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	op := m.findCallbackByID(callbackID)
	if op == nil {
		return fmt.Errorf("durabletest: callback %q not found", callbackID)
	}
	if op.Status != durable.OperationStatusStarted {
		return fmt.Errorf("durabletest: callback %q is in %s status, expected STARTED", callbackID, op.Status)
	}

	updated := *op
	switch result.status {
	case statusSucceeded:
		updated.Status = durable.OperationStatusSucceeded
		if updated.CallbackDetails == nil {
			updated.CallbackDetails = &durable.CallbackDetails{}
		}
		cd := *updated.CallbackDetails
		cd.Result = strptr(result.result)
		updated.CallbackDetails = &cd
		now := m.stampTransition(&updated)
		m.recordOperationEvent(&updated, types.EventTypeCallbackSucceeded, cd.Result, nil, now)
	case statusFailed:
		updated.Status = durable.OperationStatusFailed
		if updated.CallbackDetails == nil {
			updated.CallbackDetails = &durable.CallbackDetails{}
		}
		cd := *updated.CallbackDetails
		cd.Error = &durable.ErrorObject{
			ErrorType:    strptr(result.errType),
			ErrorMessage: strptr(result.errMsg),
		}
		if result.errData != "" {
			cd.Error.ErrorData = strptr(result.errData)
		}
		if len(result.stackTrace) > 0 {
			cd.Error.StackTrace = append([]string(nil), result.stackTrace...)
		}
		updated.CallbackDetails = &cd
		now := m.stampTransition(&updated)
		m.recordOperationEvent(&updated, types.EventTypeCallbackFailed, nil, cd.Error, now)
	default:
		return fmt.Errorf("durabletest: unsupported callback result status %q", result.status)
	}

	m.operations[ptrStr(op.Id)] = &updated
	m.markChangedLocked(ptrStr(op.Id))
	return nil
}

// timeoutCallback transitions a STARTED callback operation to TIMED_OUT,
// simulating the backend behavior when a callback's timeout elapses.
func (m *memoryClient) timeoutCallback(callbackID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	op := m.findCallbackByID(callbackID)
	if op == nil {
		return fmt.Errorf("durabletest: callback %q not found", callbackID)
	}
	if op.Status != durable.OperationStatusStarted {
		return fmt.Errorf("durabletest: callback %q is in %s status, expected STARTED", callbackID, op.Status)
	}

	updated := *op
	updated.Status = durable.OperationStatusTimedOut
	now := m.stampTransition(&updated)
	m.operations[ptrStr(op.Id)] = &updated
	m.markChangedLocked(ptrStr(op.Id))
	m.recordOperationEvent(&updated, types.EventTypeCallbackTimedOut, nil, &durable.ErrorObject{
		ErrorType:    strptr(errTypeCallbackTimedOut),
		ErrorMessage: strptr("callback timed out before it was resolved"),
	}, now)
	return nil
}

// heartbeatCallback validates that a callback exists and is in STARTED
// status. No actual timeout mechanics are simulated.
func (m *memoryClient) heartbeatCallback(callbackID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	op := m.findCallbackByID(callbackID)
	if op == nil {
		return fmt.Errorf("durabletest: callback %q not found", callbackID)
	}
	if op.Status != durable.OperationStatusStarted {
		return fmt.Errorf("durabletest: callback %q is in %s status, expected STARTED", callbackID, op.Status)
	}
	return nil
}

// openCallbacks returns all CALLBACK operations in STARTED status.
func (m *memoryClient) openCallbacks() []OpenCallback {
	m.mu.Lock()
	defer m.mu.Unlock()

	var cbs []OpenCallback
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if op.Type == durable.OperationTypeCallback && op.Status == durable.OperationStatusStarted {
			cbID := ""
			if op.CallbackDetails != nil && op.CallbackDetails.CallbackId != nil {
				cbID = *op.CallbackDetails.CallbackId
			}
			cbs = append(cbs, OpenCallback{
				CallbackID: cbID,
				Name:       ptrStr(op.Name),
			})
		}
	}
	return cbs
}

// completeChainedInvoke applies a result to a chained-invoke operation
// identified by its operation name.
func (m *memoryClient) completeChainedInvoke(name string, result operationResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	op := m.findByName(name)
	if op == nil {
		return fmt.Errorf("durabletest: chained-invoke operation %q not found", name)
	}
	if op.Type != durable.OperationTypeChainedInvoke {
		return fmt.Errorf("durabletest: operation %q is %s, not CHAINED_INVOKE", name, op.Type)
	}
	if op.Status != durable.OperationStatusStarted {
		return fmt.Errorf("durabletest: chained-invoke %q is in %s status, expected STARTED", name, op.Status)
	}
	return m.settleInvoke(op, result)
}

// settleInvokeByID applies a result to the STARTED chained-invoke operation
// with the given operation ID. The runner calls it after a registered
// function has produced the invoke's outcome.
func (m *memoryClient) settleInvokeByID(id string, result operationResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	op := m.operations[id]
	if op == nil || op.Type != durable.OperationTypeChainedInvoke {
		return fmt.Errorf("durabletest: chained-invoke operation %q not found", id)
	}
	if op.Status != durable.OperationStatusStarted {
		return fmt.Errorf("durabletest: chained-invoke %q is in %s status, expected STARTED", id, op.Status)
	}
	return m.settleInvoke(op, result)
}

// settleInvoke writes a terminal outcome onto a chained-invoke operation.
// Caller must hold m.mu.
func (m *memoryClient) settleInvoke(op *durable.Operation, result operationResult) error {
	updated := *op
	switch result.status {
	case statusSucceeded:
		updated.Status = durable.OperationStatusSucceeded
		updated.ChainedInvokeDetails = &durable.ChainedInvokeDetails{
			Result: strptr(result.result),
		}
		now := m.stampTransition(&updated)
		m.recordOperationEvent(&updated, types.EventTypeChainedInvokeSucceeded, updated.ChainedInvokeDetails.Result, nil, now)
	case statusFailed:
		updated.Status = durable.OperationStatusFailed
		errObj := &durable.ErrorObject{
			ErrorType:    strptr(result.errType),
			ErrorMessage: strptr(result.errMsg),
		}
		if result.errData != "" {
			errObj.ErrorData = strptr(result.errData)
		}
		if len(result.stackTrace) > 0 {
			errObj.StackTrace = append([]string(nil), result.stackTrace...)
		}
		updated.ChainedInvokeDetails = &durable.ChainedInvokeDetails{Error: errObj}
		now := m.stampTransition(&updated)
		m.recordOperationEvent(&updated, types.EventTypeChainedInvokeFailed, nil, errObj, now)
	case statusTimedOut:
		updated.Status = durable.OperationStatusTimedOut
		errObj := &durable.ErrorObject{
			ErrorType:    strptr(result.errType),
			ErrorMessage: strptr(result.errMsg),
		}
		updated.ChainedInvokeDetails = &durable.ChainedInvokeDetails{Error: errObj}
		now := m.stampTransition(&updated)
		m.recordOperationEvent(&updated, types.EventTypeChainedInvokeTimedOut, nil, errObj, now)
	default:
		return fmt.Errorf("durabletest: unsupported chained-invoke result status %q", result.status)
	}

	m.operations[ptrStr(op.Id)] = &updated
	m.markChangedLocked(ptrStr(op.Id))
	return nil
}

// openInvoke is a STARTED chained-invoke operation together with the
// target it asked for.
type openInvoke struct {
	id     string
	name   string
	target invokeTarget
}

// openInvokes returns every CHAINED_INVOKE operation in STARTED status, in
// insertion order.
func (m *memoryClient) openInvokes() []openInvoke {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []openInvoke
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil || op.Type != durable.OperationTypeChainedInvoke {
			continue
		}
		if op.Status != durable.OperationStatusStarted {
			continue
		}
		out = append(out, openInvoke{id: id, name: ptrStr(op.Name), target: m.invokeTargets[id]})
	}
	return out
}

// deepCopyOperation creates a fully independent copy of a durable.Operation.
// All pointer fields and nested structs are cloned so the copy shares no
// memory with the original.
func deepCopyOperation(src durable.Operation) durable.Operation {
	dst := durable.Operation{
		Id:       copyStringPtr(src.Id),
		Status:   src.Status,
		Type:     src.Type,
		Name:     copyStringPtr(src.Name),
		SubType:  copyStringPtr(src.SubType),
		ParentId: copyStringPtr(src.ParentId),
	}
	if src.StartTimestamp != nil {
		t := *src.StartTimestamp
		dst.StartTimestamp = &t
	}
	if src.EndTimestamp != nil {
		t := *src.EndTimestamp
		dst.EndTimestamp = &t
	}
	if sd := src.StepDetails; sd != nil {
		dst.StepDetails = &durable.StepDetails{
			Attempt: sd.Attempt,
			Result:  copyStringPtr(sd.Result),
		}
		if sd.NextAttemptTimestamp != nil {
			t := *sd.NextAttemptTimestamp
			dst.StepDetails.NextAttemptTimestamp = &t
		}
		if sd.Error != nil {
			dst.StepDetails.Error = copyErrorObject(sd.Error)
		}
	}
	if cd := src.CallbackDetails; cd != nil {
		dst.CallbackDetails = &durable.CallbackDetails{
			CallbackId: copyStringPtr(cd.CallbackId),
			Result:     copyStringPtr(cd.Result),
		}
		if cd.Error != nil {
			dst.CallbackDetails.Error = copyErrorObject(cd.Error)
		}
	}
	if id := src.ChainedInvokeDetails; id != nil {
		dst.ChainedInvokeDetails = &durable.ChainedInvokeDetails{
			Result: copyStringPtr(id.Result),
		}
		if id.Error != nil {
			dst.ChainedInvokeDetails.Error = copyErrorObject(id.Error)
		}
	}
	if cd := src.ContextDetails; cd != nil {
		dst.ContextDetails = &durable.ContextDetails{
			Result:         copyStringPtr(cd.Result),
			ReplayChildren: copyBoolPtr(cd.ReplayChildren),
		}
		if cd.Error != nil {
			dst.ContextDetails.Error = copyErrorObject(cd.Error)
		}
	}
	if src.WaitDetails != nil {
		wd := &durable.WaitDetails{}
		if src.WaitDetails.ScheduledEndTimestamp != nil {
			t := *src.WaitDetails.ScheduledEndTimestamp
			wd.ScheduledEndTimestamp = &t
		}
		dst.WaitDetails = wd
	}
	if src.ExecutionDetails != nil {
		dst.ExecutionDetails = &durable.ExecutionDetails{
			InputPayload: copyStringPtr(src.ExecutionDetails.InputPayload),
		}
	}
	return dst
}

// copyErrorObject deep-copies an ErrorObject including its StackTrace slice.
func copyErrorObject(src *durable.ErrorObject) *durable.ErrorObject {
	dst := &durable.ErrorObject{
		ErrorData:    copyStringPtr(src.ErrorData),
		ErrorMessage: copyStringPtr(src.ErrorMessage),
		ErrorType:    copyStringPtr(src.ErrorType),
	}
	if len(src.StackTrace) > 0 {
		dst.StackTrace = make([]string, len(src.StackTrace))
		copy(dst.StackTrace, src.StackTrace)
	}
	return dst
}

// copyStringPtr returns a pointer to a copy of the string, or nil.
func copyStringPtr(p *string) *string {
	if p == nil {
		return nil
	}
	s := *p
	return &s
}

// copyBoolPtr returns a pointer to a copy of the bool, or nil.
func copyBoolPtr(p *bool) *bool {
	if p == nil {
		return nil
	}
	b := *p
	return &b
}

// findCallbackByID locates a callback operation by its callback ID.
// Caller must hold m.mu.
func (m *memoryClient) findCallbackByID(callbackID string) *durable.Operation {
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if op.Type == durable.OperationTypeCallback && op.CallbackDetails != nil {
			if ptrStr(op.CallbackDetails.CallbackId) == callbackID {
				return op
			}
		}
	}
	return nil
}

// findByName locates an operation by its user-supplied name.
// Caller must hold m.mu.
func (m *memoryClient) findByName(name string) *durable.Operation {
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if ptrStr(op.Name) == name {
			return op
		}
	}
	return nil
}
