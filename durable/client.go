// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"context"
	"time"
)

// ExecutionClient persists and retrieves durable execution state. The SDK
// calls it to load the checkpointed operation log at the start of an
// invocation and to record operation updates as the handler runs.
//
// The default implementation calls the AWS Lambda service. The
// [durabletest] package provides an in-memory implementation for local
// testing without AWS infrastructure. Custom implementations are injected
// with [WithExecutionClient].
//
// All types in the interface are owned by this SDK, so implementing it
// requires no dependency on any AWS SDK module.
type ExecutionClient interface {
	// GetExecutionState returns one page of the execution's checkpointed
	// operation log. Set [GetExecutionStateInput.Marker] from a previous
	// page's NextMarker to continue pagination.
	GetExecutionState(ctx context.Context, in GetExecutionStateInput) (GetExecutionStateOutput, error)

	// Checkpoint atomically applies operation updates and rotates the
	// checkpoint token. The returned state carries the updated operations,
	// including any backend-assigned fields (such as callback IDs).
	Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error)
}

// GetExecutionStateInput requests one page of an execution's checkpointed
// operation log.
type GetExecutionStateInput struct {
	// ExecutionArn identifies the durable execution.
	ExecutionArn string

	// CheckpointToken is the caller's current checkpoint token.
	CheckpointToken string

	// Marker continues pagination from a previous page's NextMarker.
	// Empty requests the first page.
	Marker string
}

// GetExecutionStateOutput is one page of an execution's checkpointed
// operation log.
type GetExecutionStateOutput struct {
	// Operations are the checkpointed operations on this page.
	Operations []Operation

	// NextMarker is non-empty when more pages remain. Pass it as the next
	// request's Marker.
	NextMarker string
}

// CheckpointInput applies a batch of operation updates to an execution.
type CheckpointInput struct {
	// ExecutionArn identifies the durable execution.
	ExecutionArn string

	// CheckpointToken is the caller's current checkpoint token. The
	// backend rejects stale tokens, which serializes checkpoint writers.
	CheckpointToken string

	// Updates are the operation updates to apply atomically.
	Updates []OperationUpdate
}

// CheckpointOutput is the result of a successful checkpoint call.
type CheckpointOutput struct {
	// CheckpointToken is the rotated token to use for subsequent calls.
	CheckpointToken string

	// NewExecutionState carries the updated operations from the backend,
	// including backend-assigned fields (such as callback IDs).
	NewExecutionState []Operation
}

// OperationType identifies the kind of a durable operation.
type OperationType string

// Operation types.
const (
	OperationTypeExecution     OperationType = "EXECUTION"
	OperationTypeContext       OperationType = "CONTEXT"
	OperationTypeStep          OperationType = "STEP"
	OperationTypeWait          OperationType = "WAIT"
	OperationTypeCallback      OperationType = "CALLBACK"
	OperationTypeChainedInvoke OperationType = "CHAINED_INVOKE"
)

// OperationAction is the state transition a checkpoint update applies to
// an operation.
type OperationAction string

// Operation actions.
const (
	OperationActionStart   OperationAction = "START"
	OperationActionSucceed OperationAction = "SUCCEED"
	OperationActionFail    OperationAction = "FAIL"
	OperationActionRetry   OperationAction = "RETRY"
	OperationActionCancel  OperationAction = "CANCEL"
)

// In-progress operation statuses, plus the successful terminal status.
// The failure terminal statuses are declared in errors.go alongside
// [InvokeError], which reports them.
const (
	// OperationStatusStarted indicates the operation is in progress.
	OperationStatusStarted OperationStatus = "STARTED"

	// OperationStatusPending indicates the operation is awaiting a timer
	// (such as a step retry delay).
	OperationStatusPending OperationStatus = "PENDING"

	// OperationStatusReady indicates the operation's timer elapsed and it
	// is ready to run again.
	OperationStatusReady OperationStatus = "READY"

	// OperationStatusSucceeded indicates the operation completed with a
	// result.
	OperationStatusSucceeded OperationStatus = "SUCCEEDED"
)

// Operation is one checkpointed durable operation record. Exactly one of
// the details fields is set, matching Type.
//
// Optional string fields are pointers so that an absent value is
// distinguishable from an empty one, mirroring the checkpoint wire
// contract.
type Operation struct {
	// Id is the operation's unique identifier.
	Id *string

	// Status is the operation's current status.
	Status OperationStatus

	// Type is the operation's kind.
	Type OperationType

	// SubType further qualifies the operation's kind.
	SubType *string

	// Name is the caller-supplied operation name.
	Name *string

	// ParentId identifies the parent operation for operations inside a
	// child context.
	ParentId *string

	// StartTimestamp is when the operation started.
	StartTimestamp *time.Time

	// EndTimestamp is when the operation reached a terminal status.
	EndTimestamp *time.Time

	// ExecutionDetails is set for EXECUTION operations.
	ExecutionDetails *ExecutionDetails

	// StepDetails is set for STEP operations.
	StepDetails *StepDetails

	// WaitDetails is set for WAIT operations.
	WaitDetails *WaitDetails

	// CallbackDetails is set for CALLBACK operations.
	CallbackDetails *CallbackDetails

	// ChainedInvokeDetails is set for CHAINED_INVOKE operations.
	ChainedInvokeDetails *ChainedInvokeDetails

	// ContextDetails is set for CONTEXT operations.
	ContextDetails *ContextDetails
}

// OperationUpdate is one state transition to apply to an operation in a
// checkpoint call.
type OperationUpdate struct {
	// Id is the operation's unique identifier.
	Id *string

	// Type is the operation's kind.
	Type OperationType

	// Action is the state transition to apply.
	Action OperationAction

	// SubType further qualifies the operation's kind.
	SubType *string

	// Name is the caller-supplied operation name.
	Name *string

	// ParentId identifies the parent operation for operations inside a
	// child context.
	ParentId *string

	// Payload is the operation result for SUCCEED actions.
	Payload *string

	// Error carries failure details for FAIL and RETRY actions.
	Error *ErrorObject

	// StepOptions configures STEP operations.
	StepOptions *StepOptions

	// WaitOptions configures WAIT operations.
	WaitOptions *WaitOptions

	// CallbackOptions configures CALLBACK operations.
	CallbackOptions *CallbackOptions

	// ChainedInvokeOptions configures CHAINED_INVOKE operations.
	ChainedInvokeOptions *ChainedInvokeOptions

	// ContextOptions configures CONTEXT operations.
	ContextOptions *ContextOptions
}

// ErrorObject carries structured error information for a failed operation.
type ErrorObject struct {
	// ErrorType is the error's type name.
	ErrorType *string

	// ErrorMessage is the human-readable error message.
	ErrorMessage *string

	// ErrorData is machine-readable error data.
	ErrorData *string

	// StackTrace is optional stack trace information.
	StackTrace []string
}

// ExecutionDetails carries EXECUTION operation state.
type ExecutionDetails struct {
	// InputPayload is the original input to the durable execution.
	InputPayload *string
}

// StepDetails carries STEP operation state.
type StepDetails struct {
	// Attempt is the current attempt number.
	Attempt int32

	// Result is the serialized step result, set on success.
	Result *string

	// Error carries failure details, set on failure or pending retry.
	Error *ErrorObject

	// NextAttemptTimestamp is when the next retry attempt is scheduled.
	// Set only while the step is pending.
	NextAttemptTimestamp *time.Time
}

// WaitDetails carries WAIT operation state.
type WaitDetails struct {
	// ScheduledEndTimestamp is when the wait completes.
	ScheduledEndTimestamp *time.Time
}

// CallbackDetails carries CALLBACK operation state.
type CallbackDetails struct {
	// CallbackId is the identifier external systems use to resolve the
	// callback. It is assigned by the backend when the callback starts.
	CallbackId *string

	// Result is the payload the external system submitted, set on success.
	Result *string

	// Error carries failure details, set on failure.
	Error *ErrorObject
}

// ChainedInvokeDetails carries CHAINED_INVOKE operation state.
type ChainedInvokeDetails struct {
	// Result is the invoked function's serialized result, set on success.
	Result *string

	// Error carries failure details, set on failure.
	Error *ErrorObject
}

// ContextDetails carries CONTEXT operation state.
type ContextDetails struct {
	// Result is the child context's serialized result, set on success.
	Result *string

	// ReplayChildren reports whether the completed context's child
	// operations are included in replay state.
	ReplayChildren *bool

	// Error carries failure details, set on failure.
	Error *ErrorObject
}

// StepOptions configures a STEP operation update.
type StepOptions struct {
	// NextAttemptDelaySeconds is the delay before the next retry attempt.
	NextAttemptDelaySeconds *int32
}

// WaitOptions configures a WAIT operation update.
type WaitOptions struct {
	// WaitSeconds is the wait duration in seconds.
	WaitSeconds *int32
}

// CallbackOptions configures a CALLBACK operation update.
type CallbackOptions struct {
	// TimeoutSeconds bounds how long the callback waits for an external
	// submission. Zero disables the timeout.
	TimeoutSeconds int32

	// HeartbeatTimeoutSeconds bounds the interval between heartbeats.
	// Zero disables the heartbeat timeout.
	HeartbeatTimeoutSeconds int32
}

// ChainedInvokeOptions configures a CHAINED_INVOKE operation update.
type ChainedInvokeOptions struct {
	// FunctionName is the name or ARN of the function to invoke.
	FunctionName *string

	// TenantId is the tenant identifier for the chained invocation.
	TenantId *string
}

// ContextOptions configures a CONTEXT operation update.
type ContextOptions struct {
	// ReplayChildren requests that the completed context's child
	// operations be included in replay state.
	ReplayChildren *bool
}
