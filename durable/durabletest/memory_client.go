// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"fmt"
	"strconv"
	"sync"

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
}

// invokeTarget is what a chained invoke asked for: the function to run and
// the serialized input to run it with.
type invokeTarget struct {
	functionID string
	payload    string
}

func newMemoryClient() *memoryClient {
	return &memoryClient{
		token:         "test-token-0",
		operations:    make(map[string]*durable.Operation),
		invokeTargets: make(map[string]invokeTarget),
	}
}

// Checkpoint applies operation updates, rotates the checkpoint token, and
// returns the updated operations. Per the backend contract, the token
// rotates on every successful checkpoint response.
func (m *memoryClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Apply each update.
	var updated []durable.Operation
	for _, u := range in.Updates {
		op := m.applyUpdate(u)
		updated = append(updated, op)
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
		}
	}
	return out, nil
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

	// Build type-specific details.
	switch u.Type {
	case durable.OperationTypeStep:
		op.StepDetails = buildStepDetails(u, existing)
	case durable.OperationTypeWait:
		op.WaitDetails = buildWaitDetails(u)
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
	return op
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
func buildStepDetails(u durable.OperationUpdate, existing *durable.Operation) *durable.StepDetails {
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
	return sd
}

// buildWaitDetails constructs WaitDetails for a wait checkpoint update.
// The local runner does not track wait durations; all pending waits complete
// unconditionally when [LocalRunner.CompletePendingTimers] is called.
func buildWaitDetails(u durable.OperationUpdate) *durable.WaitDetails {
	if u.Action != durable.OperationActionStart {
		return nil
	}
	return &durable.WaitDetails{}
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
	statusStarted   = "STARTED"
	statusPending   = "PENDING"
	statusReady     = "READY"
)

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

// completePendingTimers transitions timer-blocked operations:
//   - STEP in PENDING → READY (retry timer elapsed)
//   - WAIT in STARTED → SUCCEEDED (wait duration elapsed)
//
// Returns true if any operation was advanced.
func (m *memoryClient) completePendingTimers() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	advanced := false
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if op.Type == durable.OperationTypeStep && op.Status == durable.OperationStatusPending {
			// PENDING → READY: retry timer elapsed.
			updated := *op
			updated.Status = durable.OperationStatusReady
			m.operations[id] = &updated
			advanced = true
		}
		if op.Type == durable.OperationTypeWait && op.Status == durable.OperationStatusStarted {
			// STARTED → SUCCEEDED: wait elapsed.
			updated := *op
			updated.Status = durable.OperationStatusSucceeded
			m.operations[id] = &updated
			advanced = true
		}
	}
	return advanced
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
		updated.CallbackDetails = &cd
	default:
		return fmt.Errorf("durabletest: unsupported callback result status %q", result.status)
	}

	m.operations[ptrStr(op.Id)] = &updated
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
	m.operations[ptrStr(op.Id)] = &updated
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
	default:
		return fmt.Errorf("durabletest: unsupported chained-invoke result status %q", result.status)
	}

	m.operations[ptrStr(op.Id)] = &updated
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
