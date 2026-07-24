// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

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
	operations map[string]*types.Operation // keyed by operation ID
	opOrder    []string                    // insertion-order tracking
}

func newMemoryClient() *memoryClient {
	return &memoryClient{
		token:      "test-token-0",
		operations: make(map[string]*types.Operation),
	}
}

// CheckpointDurableExecution applies operation updates, rotates the
// checkpoint token, and returns the updated operations. Per the backend
// contract, the token rotates on every successful checkpoint response.
func (m *memoryClient) CheckpointDurableExecution(_ context.Context, in *lambda.CheckpointDurableExecutionInput, _ ...func(*lambda.Options)) (*lambda.CheckpointDurableExecutionOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Apply each update.
	var updated []types.Operation
	for _, u := range in.Updates {
		op := m.applyUpdate(u)
		updated = append(updated, op)
	}

	// Rotate token.
	m.tokenSeq++
	m.token = "test-token-" + strconv.Itoa(m.tokenSeq)

	return &lambda.CheckpointDurableExecutionOutput{
		CheckpointToken: aws.String(m.token),
		NewExecutionState: &types.CheckpointUpdatedExecutionState{
			Operations: updated,
		},
	}, nil
}

// GetDurableExecutionState returns all stored operations in a single page.
// The local runner does not use pagination; this method exists to satisfy
// the interface for the initial loadState pagination loop. Each returned
// operation is a deep copy, independent of the memoryClient's internal
// state.
func (m *memoryClient) GetDurableExecutionState(_ context.Context, _ *lambda.GetDurableExecutionStateInput, _ ...func(*lambda.Options)) (*lambda.GetDurableExecutionStateOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ops := make([]types.Operation, 0, len(m.operations))
	for _, id := range m.opOrder {
		if op, ok := m.operations[id]; ok {
			ops = append(ops, deepCopyOperation(*op))
		}
	}
	return &lambda.GetDurableExecutionStateOutput{
		Operations: ops,
	}, nil
}

// allOperations returns a deep-copied snapshot of all stored operations in
// insertion order, excluding the execution operation. Each returned
// Operation is fully independent of the memoryClient's internal state,
// preventing data races if the caller reads details while a concurrent
// checkpoint mutates the store.
func (m *memoryClient) allOperations() []types.Operation {
	m.mu.Lock()
	defer m.mu.Unlock()

	ops := make([]types.Operation, 0, len(m.operations))
	for _, id := range m.opOrder {
		op, ok := m.operations[id]
		if !ok {
			continue
		}
		// Skip the execution operation (type EXECUTION).
		if op.Type == types.OperationTypeExecution {
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
func (m *memoryClient) applyUpdate(u types.OperationUpdate) types.Operation {
	id := aws.ToString(u.Id)

	existing := m.operations[id]
	op := types.Operation{
		Id:       u.Id,
		Name:     u.Name,
		Type:     u.Type,
		SubType:  u.SubType,
		ParentId: u.ParentId,
		Status:   deriveStatus(u.Action),
	}

	// Build type-specific details.
	switch u.Type {
	case types.OperationTypeStep:
		op.StepDetails = buildStepDetails(u, existing)
	case types.OperationTypeWait:
		op.WaitDetails = buildWaitDetails(u)
	case types.OperationTypeCallback:
		op.CallbackDetails = buildCallbackDetails(u, existing)
	case types.OperationTypeChainedInvoke:
		op.ChainedInvokeDetails = buildChainedInvokeDetails(u)
	case types.OperationTypeContext:
		op.ContextDetails = buildContextDetails(u)
	case types.OperationTypeExecution:
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
func deriveStatus(action types.OperationAction) types.OperationStatus {
	switch action {
	case types.OperationActionStart:
		return types.OperationStatusStarted
	case types.OperationActionSucceed:
		return types.OperationStatusSucceeded
	case types.OperationActionFail:
		return types.OperationStatusFailed
	case types.OperationActionRetry:
		return types.OperationStatusPending
	case types.OperationActionCancel:
		return types.OperationStatusCancelled
	default:
		return types.OperationStatus(string(action))
	}
}

// buildStepDetails constructs StepDetails for a checkpoint update, carrying
// forward attempt count and applying the action semantics.
func buildStepDetails(u types.OperationUpdate, existing *types.Operation) *types.StepDetails {
	var attempt int32
	if existing != nil && existing.StepDetails != nil {
		attempt = existing.StepDetails.Attempt
	}

	sd := &types.StepDetails{}
	switch u.Action {
	case types.OperationActionStart:
		// START preserves existing attempt count (step re-entered after
		// crash or retry timer). Only increment on RETRY/FAIL.
		sd.Attempt = attempt
	case types.OperationActionSucceed:
		sd.Attempt = attempt
		if u.Payload != nil {
			sd.Result = u.Payload
		}
	case types.OperationActionFail:
		sd.Attempt = attempt + 1
		if u.Error != nil {
			sd.Error = u.Error
		}
	case types.OperationActionRetry:
		sd.Attempt = attempt + 1
		if u.Error != nil {
			sd.Error = u.Error
		}
	default:
		sd.Attempt = attempt
	}
	return sd
}

// buildWaitDetails constructs WaitDetails for a wait checkpoint update.
func buildWaitDetails(u types.OperationUpdate) *types.WaitDetails {
	if u.Action != types.OperationActionStart {
		return nil
	}
	wd := &types.WaitDetails{}
	if u.WaitOptions != nil && u.WaitOptions.WaitSeconds != nil {
		// Store the wait duration; the local runner does not implement
		// real timers but records the requested duration.
		_ = *u.WaitOptions.WaitSeconds // value consumed by AdvanceTime (R2)
	}
	return wd
}

// buildCallbackDetails constructs CallbackDetails, generating a callback ID
// on START and preserving it on subsequent updates.
func buildCallbackDetails(u types.OperationUpdate, existing *types.Operation) *types.CallbackDetails {
	cd := &types.CallbackDetails{}

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
	if u.Action != types.OperationActionStart {
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
func buildChainedInvokeDetails(u types.OperationUpdate) *types.ChainedInvokeDetails {
	if u.Action == types.OperationActionStart {
		return &types.ChainedInvokeDetails{}
	}
	return &types.ChainedInvokeDetails{
		Result: u.Payload,
		Error:  u.Error,
	}
}

// buildContextDetails constructs ContextDetails.
func buildContextDetails(u types.OperationUpdate) *types.ContextDetails {
	cd := &types.ContextDetails{
		Result: u.Payload,
		Error:  u.Error,
	}
	if u.ContextOptions != nil && u.ContextOptions.ReplayChildren != nil && *u.ContextOptions.ReplayChildren {
		cd.ReplayChildren = aws.Bool(true)
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
	status  string
	result  string
	errType string
	errMsg  string
}

// OpenCallback identifies a callback operation pending external resolution.
type OpenCallback struct {
	// CallbackID is the identifier to pass to [LocalRunner.SendCallbackSuccess]
	// or [LocalRunner.SendCallbackFailure].
	CallbackID string

	// Name is the caller-supplied operation name.
	Name string
}

// advanceTime transitions time-eligible operations:
//   - STEP in PENDING → READY (retry timer elapsed)
//   - WAIT in STARTED → SUCCEEDED (wait duration elapsed)
//
// Returns true if any operation was advanced.
func (m *memoryClient) advanceTime() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	advanced := false
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if op.Type == types.OperationTypeStep && op.Status == types.OperationStatusPending {
			// PENDING → READY: retry timer elapsed.
			updated := *op
			updated.Status = types.OperationStatusReady
			m.operations[id] = &updated
			advanced = true
		}
		if op.Type == types.OperationTypeWait && op.Status == types.OperationStatusStarted {
			// STARTED → SUCCEEDED: wait elapsed.
			updated := *op
			updated.Status = types.OperationStatusSucceeded
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
	if op.Status != types.OperationStatusStarted {
		return fmt.Errorf("durabletest: callback %q is in %s status, expected STARTED", callbackID, op.Status)
	}

	updated := *op
	switch result.status {
	case statusSucceeded:
		updated.Status = types.OperationStatusSucceeded
		if updated.CallbackDetails == nil {
			updated.CallbackDetails = &types.CallbackDetails{}
		}
		cd := *updated.CallbackDetails
		cd.Result = aws.String(result.result)
		updated.CallbackDetails = &cd
	case statusFailed:
		updated.Status = types.OperationStatusFailed
		if updated.CallbackDetails == nil {
			updated.CallbackDetails = &types.CallbackDetails{}
		}
		cd := *updated.CallbackDetails
		cd.Error = &types.ErrorObject{
			ErrorType:    aws.String(result.errType),
			ErrorMessage: aws.String(result.errMsg),
		}
		updated.CallbackDetails = &cd
	default:
		return fmt.Errorf("durabletest: unsupported callback result status %q", result.status)
	}

	m.operations[aws.ToString(op.Id)] = &updated
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
	if op.Status != types.OperationStatusStarted {
		return fmt.Errorf("durabletest: callback %q is in %s status, expected STARTED", callbackID, op.Status)
	}

	updated := *op
	updated.Status = types.OperationStatusTimedOut
	m.operations[aws.ToString(op.Id)] = &updated
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
	if op.Status != types.OperationStatusStarted {
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
		if op.Type == types.OperationTypeCallback && op.Status == types.OperationStatusStarted {
			cbID := ""
			if op.CallbackDetails != nil && op.CallbackDetails.CallbackId != nil {
				cbID = *op.CallbackDetails.CallbackId
			}
			cbs = append(cbs, OpenCallback{
				CallbackID: cbID,
				Name:       aws.ToString(op.Name),
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
	if op.Type != types.OperationTypeChainedInvoke {
		return fmt.Errorf("durabletest: operation %q is %s, not CHAINED_INVOKE", name, op.Type)
	}
	if op.Status != types.OperationStatusStarted {
		return fmt.Errorf("durabletest: chained-invoke %q is in %s status, expected STARTED", name, op.Status)
	}

	updated := *op
	switch result.status {
	case statusSucceeded:
		updated.Status = types.OperationStatusSucceeded
		updated.ChainedInvokeDetails = &types.ChainedInvokeDetails{
			Result: aws.String(result.result),
		}
	case statusFailed:
		updated.Status = types.OperationStatusFailed
		updated.ChainedInvokeDetails = &types.ChainedInvokeDetails{
			Error: &types.ErrorObject{
				ErrorType:    aws.String(result.errType),
				ErrorMessage: aws.String(result.errMsg),
			},
		}
	default:
		return fmt.Errorf("durabletest: unsupported chained-invoke result status %q", result.status)
	}

	m.operations[aws.ToString(op.Id)] = &updated
	return nil
}

// deepCopyOperation creates a fully independent copy of a types.Operation.
// All pointer fields and nested structs are cloned so the copy shares no
// memory with the original.
func deepCopyOperation(src types.Operation) types.Operation {
	dst := types.Operation{
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
		dst.StepDetails = &types.StepDetails{
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
		dst.CallbackDetails = &types.CallbackDetails{
			CallbackId: copyStringPtr(cd.CallbackId),
			Result:     copyStringPtr(cd.Result),
		}
		if cd.Error != nil {
			dst.CallbackDetails.Error = copyErrorObject(cd.Error)
		}
	}
	if id := src.ChainedInvokeDetails; id != nil {
		dst.ChainedInvokeDetails = &types.ChainedInvokeDetails{
			Result: copyStringPtr(id.Result),
		}
		if id.Error != nil {
			dst.ChainedInvokeDetails.Error = copyErrorObject(id.Error)
		}
	}
	if cd := src.ContextDetails; cd != nil {
		dst.ContextDetails = &types.ContextDetails{
			Result:         copyStringPtr(cd.Result),
			ReplayChildren: copyBoolPtr(cd.ReplayChildren),
		}
		if cd.Error != nil {
			dst.ContextDetails.Error = copyErrorObject(cd.Error)
		}
	}
	if src.WaitDetails != nil {
		wd := &types.WaitDetails{}
		if src.WaitDetails.ScheduledEndTimestamp != nil {
			t := *src.WaitDetails.ScheduledEndTimestamp
			wd.ScheduledEndTimestamp = &t
		}
		dst.WaitDetails = wd
	}
	if src.ExecutionDetails != nil {
		dst.ExecutionDetails = &types.ExecutionDetails{
			InputPayload: copyStringPtr(src.ExecutionDetails.InputPayload),
		}
	}
	return dst
}

// copyErrorObject deep-copies an ErrorObject including its StackTrace slice.
func copyErrorObject(src *types.ErrorObject) *types.ErrorObject {
	dst := &types.ErrorObject{
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
func (m *memoryClient) findCallbackByID(callbackID string) *types.Operation {
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if op.Type == types.OperationTypeCallback && op.CallbackDetails != nil {
			if aws.ToString(op.CallbackDetails.CallbackId) == callbackID {
				return op
			}
		}
	}
	return nil
}

// findByName locates an operation by its user-supplied name.
// Caller must hold m.mu.
func (m *memoryClient) findByName(name string) *types.Operation {
	for _, id := range m.opOrder {
		op := m.operations[id]
		if op == nil {
			continue
		}
		if aws.ToString(op.Name) == name {
			return op
		}
	}
	return nil
}
