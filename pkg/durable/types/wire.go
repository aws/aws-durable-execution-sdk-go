package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// This file defines the wire-protocol types exchanged with the real AWS
// Lambda Durable Functions backend. Shapes here are taken directly from
// the official AWS API Reference pages for CheckpointDurableExecution and
// GetDurableExecutionState
// (docs.aws.amazon.com/lambda/latest/api/API_CheckpointDurableExecution.html,
// API_GetDurableExecutionState.html) - the authoritative source, as
// opposed to `aws lambda ... help` CLI text (which documents the request
// shape well but is less precise about the response shape) or
// inference from a single captured invocation payload. See
// docs/checkpoint-replay-design.md for the verification history. These
// types are fixed by the backend and MUST remain consistent with the
// JS/Python/Java SDKs.
//
// Two distinct shapes exist and must not be conflated:
//   - OperationUpdate (request side, sent in CheckpointDurableExecution's
//     Updates[]): flat fields plus per-action *Options structs
//     (StepOptions, WaitOptions, CallbackOptions, ContextOptions,
//     ChainedInvokeOptions).
//   - Operation (response side, returned by both APIs' Operations[]):
//     flat identity/status fields plus per-TYPE *Details structs
//     (ExecutionDetails, StepDetails, WaitDetails, CallbackDetails,
//     ChainedInvokeDetails, ContextDetails) - NOT the same shape as the
//     *Options structs, despite superficially similar names.

// DurableExecutionInvocationInput is the payload the Lambda Durable
// Functions backend delivers on each invocation of a durable function.
// Field names and shape are confirmed from a real invocation's raw event
// payload (captured via CloudWatch Logs from a deployed Go
// container-image durable function - see
// docs/checkpoint-replay-design.md): DurableExecutionArn, CheckpointToken,
// InitialExecutionState.Operations[], and UpdatedOperationIds are exactly
// the observed top-level keys.
type DurableExecutionInvocationInput struct {
	DurableExecutionArn   string                `json:"DurableExecutionArn"`
	CheckpointToken       string                `json:"CheckpointToken"`
	InitialExecutionState InitialExecutionState `json:"InitialExecutionState"`

	// UpdatedOperationIds lists the IDs of operations that changed since
	// the last invocation (e.g. a callback the external system completed
	// while this invocation was suspended). Confirmed present on the wire
	// even on a fresh execution's first invocation, where it lists the
	// root EXECUTION operation's own ID.
	UpdatedOperationIds []string `json:"UpdatedOperationIds,omitempty"`
}

// Time wraps time.Time to unmarshal BOTH timestamp encodings confirmed
// present on the real wire, in different places:
//
//   - DurableExecutionInvocationInput.InitialExecutionState.Operations[].
//     StartTimestamp arrives as a unix-millis integer (confirmed from a
//     live invocation's raw event payload:
//     "StartTimestamp":1784236727585).
//   - CheckpointDurableExecutionResponse.NewExecutionState.Operations[].
//     StartTimestamp/EndTimestamp/StepDetails.NextAttemptTimestamp/
//     WaitDetails.ScheduledEndTimestamp arrive as ISO 8601 date-time
//     strings (confirmed from a live checkpoint response via this SDK's
//     own awscli.Client: "2026-07-16T21:19:08.358000+00:00").
//
// Both are genuinely observed, not assumed - see
// docs/checkpoint-replay-design.md for the verification history. Rather
// than modeling two separate types for what the API reference documents
// as the same logical field, this type accepts either shape on
// unmarshal.
type Time struct {
	time.Time
}

const timeLayout = "2006-01-02T15:04:05.999999-07:00"

func (t *Time) UnmarshalJSON(data []byte) error {
	// Try integer (unix millis) first - this is what
	// DurableExecutionInvocationInput uses.
	var millis int64
	if err := json.Unmarshal(data, &millis); err == nil {
		t.Time = time.UnixMilli(millis)
		return nil
	}

	// Fall back to ISO 8601 string - this is what checkpoint responses use.
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("types.Time: expected a unix-millis integer or an ISO 8601 string, got %s: %w", string(data), err)
	}
	parsed, err := time.Parse(timeLayout, s)
	if err != nil {
		return fmt.Errorf("types.Time: parsing %q: %w", s, err)
	}
	t.Time = parsed
	return nil
}

// MarshalJSON always writes the ISO 8601 string form, matching the
// checkpoint-response encoding, since Time values constructed by this
// SDK's own runtime code (as opposed to unmarshaled from a raw
// invocation input) are for outgoing/logged use, not for re-encoding an
// invocation input.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Format(timeLayout))
}

// InitialExecutionState carries the prior operations recorded for this
// execution, used to drive replay-skip logic.
type InitialExecutionState struct {
	Operations []Operation `json:"Operations"`
}

// OperationType identifies the kind of durable operation an entry
// represents. Values confirmed from a live invocation payload: EXECUTION
// | CONTEXT | STEP | WAIT | CALLBACK | CHAINED_INVOKE.
type OperationType string

const (
	OperationTypeExecution     OperationType = "EXECUTION"
	OperationTypeContext       OperationType = "CONTEXT"
	OperationTypeStep          OperationType = "STEP"
	OperationTypeWait          OperationType = "WAIT"
	OperationTypeCallback      OperationType = "CALLBACK"
	OperationTypeChainedInvoke OperationType = "CHAINED_INVOKE"
)

// OperationAction identifies what a checkpoint update is asking the
// backend to do to an operation. Values confirmed from the official API
// reference for OperationUpdate.Action: START | SUCCEED | FAIL | RETRY |
// CANCEL.
type OperationAction string

const (
	OperationActionStart   OperationAction = "START"
	OperationActionSucceed OperationAction = "SUCCEED"
	OperationActionFail    OperationAction = "FAIL"
	OperationActionRetry   OperationAction = "RETRY"
	OperationActionCancel  OperationAction = "CANCEL"
)

// OperationStatus reports the lifecycle state of a checkpointed operation
// as returned by GetDurableExecutionState/CheckpointDurableExecution.
// STARTED is confirmed from a live invocation payload (the root EXECUTION
// operation's Status while in flight); the remaining values match the
// terminal/pending status set documented consistently across the
// JS/Java reference SDKs (see docs/checkpoint-replay-design.md §2).
// READY was confirmed missing from this list (compared against the AWS
// SDK for Go v2's own independently-generated lambdatypes.OperationStatus
// enum, which has eight values to this file's original seven) while
// building pkg/durable/awssdk - added here for full parity; it denotes
// an operation (e.g. a STEP retry) whose delay has elapsed and is ready
// to run again, distinct from PENDING (still waiting) and STARTED
// (currently running).
type OperationStatus string

const (
	OperationStatusStarted   OperationStatus = "STARTED"
	OperationStatusPending   OperationStatus = "PENDING"
	OperationStatusReady     OperationStatus = "READY"
	OperationStatusSucceeded OperationStatus = "SUCCEEDED"
	OperationStatusFailed    OperationStatus = "FAILED"
	OperationStatusCancelled OperationStatus = "CANCELLED"
	OperationStatusStopped   OperationStatus = "STOPPED"
	OperationStatusTimedOut  OperationStatus = "TIMED_OUT"
)

// IsTerminal reports whether s is a terminal status (no further state
// transitions expected without external intervention).
func (s OperationStatus) IsTerminal() bool {
	switch s {
	case OperationStatusSucceeded, OperationStatusFailed, OperationStatusCancelled, OperationStatusStopped, OperationStatusTimedOut:
		return true
	default:
		return false
	}
}

// ErrorObject is the structured error shape used throughout the API
// (OperationUpdate.Error, and every per-type *Details.Error on the
// response side), matching the official API reference's ErrorObject type
// exactly: ErrorMessage, ErrorType, ErrorData, StackTrace.
type ErrorObject struct {
	ErrorMessage string   `json:"ErrorMessage,omitempty"`
	ErrorType    string   `json:"ErrorType,omitempty"`
	ErrorData    string   `json:"ErrorData,omitempty"`
	StackTrace   []string `json:"StackTrace,omitempty"`
}

// OperationError is an alias retained for readability at call sites that
// predate this file's alignment with the official API's "ErrorObject"
// naming; both names refer to the same wire shape.
type OperationError = ErrorObject

// Operation is a single entry as returned by GetDurableExecutionState or
// CheckpointDurableExecution's NewExecutionState.Operations[]. This is
// the RESPONSE shape: per-type *Details structs (NOT the *Options structs
// used on the request/OperationUpdate side - see this file's top-level
// doc comment), confirmed field-for-field against the official AWS API
// Reference AND a live invocation's actual checkpoint response.
type Operation struct {
	ID       string          `json:"Id"`
	ParentID string          `json:"ParentId,omitempty"`
	Name     string          `json:"Name,omitempty"`
	Type     OperationType   `json:"Type"`
	SubType  string          `json:"SubType,omitempty"`
	Status   OperationStatus `json:"Status"`

	// StartTimestamp/EndTimestamp: confirmed from a live checkpoint
	// response to be ISO 8601 date-time strings (e.g.
	// "2026-07-16T21:19:08.358000+00:00"), NOT unix-millis integers as
	// originally assumed from the official API reference's "number" type
	// annotation (that annotation was imprecise, or the CLI's JSON
	// rendering differs from the raw REST response - unconfirmed which,
	// but the CLI-observed shape is what this Go SDK's own client
	// (pkg/durable/awscli) actually receives, so it's what must be
	// parsed). Time is used to avoid guessing a layout string wrong in
	// two places.
	StartTimestamp *Time `json:"StartTimestamp,omitempty"`
	EndTimestamp   *Time `json:"EndTimestamp,omitempty"`

	ExecutionDetails     *ExecutionDetails     `json:"ExecutionDetails,omitempty"`
	ContextDetails       *ContextDetails       `json:"ContextDetails,omitempty"`
	StepDetails          *StepDetails          `json:"StepDetails,omitempty"`
	WaitDetails          *WaitDetails          `json:"WaitDetails,omitempty"`
	CallbackDetails      *CallbackDetails      `json:"CallbackDetails,omitempty"`
	ChainedInvokeDetails *ChainedInvokeDetails `json:"ChainedInvokeDetails,omitempty"`
}

// ExecutionDetails carries the root EXECUTION operation's input payload.
// Confirmed field-for-field (InputPayload only) against the official API
// reference and a live invocation payload.
type ExecutionDetails struct {
	InputPayload *string `json:"InputPayload,omitempty"`
}

// ContextDetails carries a CONTEXT (child context) operation's outcome.
// Confirmed against the official API reference.
type ContextDetails struct {
	Result         *string      `json:"Result,omitempty"`
	Error          *ErrorObject `json:"Error,omitempty"`
	ReplayChildren bool         `json:"ReplayChildren,omitempty"`
}

// StepDetails carries a STEP operation's outcome and retry state.
// Confirmed against the official API reference; NextAttemptTimestamp
// uses the same ISO 8601 string format observed for Operation's top-level
// StartTimestamp/EndTimestamp (see Time's doc) rather than a unix-millis
// integer.
type StepDetails struct {
	Attempt              int          `json:"Attempt,omitempty"`
	Result               *string      `json:"Result,omitempty"`
	Error                *ErrorObject `json:"Error,omitempty"`
	NextAttemptTimestamp *Time        `json:"NextAttemptTimestamp,omitempty"`
}

// WaitDetails carries a WAIT operation's scheduled completion time.
// Confirmed against the official API reference (field name
// ScheduledEndTimestamp, not WaitUntilTimestamp as originally assumed
// before checking the authoritative source); uses the same ISO 8601
// string format as Operation's top-level timestamps (see Time's doc).
type WaitDetails struct {
	ScheduledEndTimestamp *Time `json:"ScheduledEndTimestamp,omitempty"`
}

// CallbackDetails carries a CALLBACK operation's ID and outcome. Confirmed
// against the official API reference.
type CallbackDetails struct {
	CallbackID string       `json:"CallbackId,omitempty"`
	Result     *string      `json:"Result,omitempty"`
	Error      *ErrorObject `json:"Error,omitempty"`
}

// ChainedInvokeDetails carries a CHAINED_INVOKE operation's outcome.
// Confirmed against the official API reference.
type ChainedInvokeDetails struct {
	Result *string      `json:"Result,omitempty"`
	Error  *ErrorObject `json:"Error,omitempty"`
}

// --- Request-side (OperationUpdate) per-action Options structs ---
// These are a DIFFERENT shape from the *Details structs above, despite
// covering similar operation types - see this file's top-level doc
// comment. Confirmed against the official API reference's
// CheckpointDurableExecution request syntax.

// ContextOptions configures a CONTEXT-type OperationUpdate.
type ContextOptions struct {
	ReplayChildren bool `json:"ReplayChildren,omitempty"`
}

// StepOptions configures a STEP-type OperationUpdate.
type StepOptions struct {
	NextAttemptDelaySeconds *int `json:"NextAttemptDelaySeconds,omitempty"`
}

// WaitOptions configures a WAIT-type OperationUpdate.
type WaitOptions struct {
	WaitSeconds *int `json:"WaitSeconds,omitempty"`
}

// CallbackOptions configures a CALLBACK-type OperationUpdate.
type CallbackOptions struct {
	TimeoutSeconds          *int `json:"TimeoutSeconds,omitempty"`
	HeartbeatTimeoutSeconds *int `json:"HeartbeatTimeoutSeconds,omitempty"`
}

// ChainedInvokeOptions configures a CHAINED_INVOKE-type OperationUpdate.
type ChainedInvokeOptions struct {
	FunctionName string `json:"FunctionName,omitempty"`
	TenantID     string `json:"TenantId,omitempty"`
}

// OperationUpdate is a single entry in a checkpoint batch: the minimal
// delta needed to advance one operation's state. Confirmed field-for-field
// against the official CheckpointDurableExecution API reference's request
// syntax.
type OperationUpdate struct {
	ID       string          `json:"Id"`
	ParentID string          `json:"ParentId,omitempty"`
	Name     string          `json:"Name,omitempty"`
	Type     OperationType   `json:"Type"`
	SubType  string          `json:"SubType,omitempty"`
	Action   OperationAction `json:"Action"`

	Payload *string      `json:"Payload,omitempty"`
	Error   *ErrorObject `json:"Error,omitempty"`

	ContextOptions       *ContextOptions       `json:"ContextOptions,omitempty"`
	StepOptions          *StepOptions          `json:"StepOptions,omitempty"`
	WaitOptions          *WaitOptions          `json:"WaitOptions,omitempty"`
	CallbackOptions      *CallbackOptions      `json:"CallbackOptions,omitempty"`
	ChainedInvokeOptions *ChainedInvokeOptions `json:"ChainedInvokeOptions,omitempty"`
}

// CheckpointDurableExecutionRequest is sent to
// POST /2025-12-01/durable-executions/{DurableExecutionArn}/checkpoint
// (confirmed exact path from the official API reference). DurableExecutionArn
// is a URI parameter on the real API, not a body field, but is kept here
// on the request struct for the Client interface's convenience - see
// checkpoint.Client implementations for how it's split back out.
type CheckpointDurableExecutionRequest struct {
	DurableExecutionArn string            `json:"-"`
	CheckpointToken     string            `json:"CheckpointToken"`
	Updates             []OperationUpdate `json:"Updates,omitempty"`
	ClientToken         *string           `json:"ClientToken,omitempty"`
}

// CheckpointDurableExecutionResponse mirrors the official API reference's
// response syntax: CheckpointToken plus NewExecutionState.Operations[].
type CheckpointDurableExecutionResponse struct {
	NextCheckpointToken *string
	UpdatedOperations   []Operation
}

// CheckpointDurableExecutionResponseWire is the literal JSON shape of a
// CheckpointDurableExecution HTTP response. Client implementations (see
// pkg/durable/sigv4lambda) unmarshal directly into this type, then map it
// to the friendlier CheckpointDurableExecutionResponse.
type CheckpointDurableExecutionResponseWire struct {
	CheckpointToken   string `json:"CheckpointToken"`
	NewExecutionState *struct {
		NextMarker string      `json:"NextMarker,omitempty"`
		Operations []Operation `json:"Operations"`
	} `json:"NewExecutionState,omitempty"`
}

// GetDurableExecutionStateRequest fetches the current recorded state of an
// execution via
// GET /2025-12-01/durable-executions/{DurableExecutionArn}/state?CheckpointToken=...&Marker=...&MaxItems=...
// (confirmed exact path and query parameters from the official API
// reference).
type GetDurableExecutionStateRequest struct {
	DurableExecutionArn string
	CheckpointToken     string
	Marker              string
	MaxItems            int
}

// GetDurableExecutionStateResponse mirrors the official API reference's
// response syntax: NextMarker plus Operations[].
type GetDurableExecutionStateResponse struct {
	NextMarker *string
	Operations []Operation
}

// DurableExecutionOutput is returned to the Lambda Durable Functions
// backend after each invocation, indicating whether the execution
// succeeded, failed, or is suspended pending a wait/callback.
//
// # Two real, root-cause wire-shape bugs found and fixed
//
// Both of the following were previously tracked as unconfirmed
// "best-effort guesses" (see this type's git history) and separately as
// two "confirmed, NOT-CLIENT-FIXABLE backend gaps" in the conformance
// test harness initiative's own docs/remaining-work.md - both
// conclusions were WRONG, and were reached without ever cross-checking
// this SDK's own guessed field names against a real, independently
// verified, working reference implementation. Direct comparison against
// the JS reference SDK's actual, tested source
// (packages/aws-durable-execution-sdk-js/src/with-durable-execution.ts's
// runHandler, and src/types/core.ts's DurableExecutionInvocationOutput*
// interfaces, both confirmed via that repo's own passing
// with-durable-execution.test.ts assertions - e.g. `expect(response).
// toEqual({Status: InvocationStatus.SUCCEEDED, Result:
// JSON.stringify(mockResult)})`) reveals this Go SDK was sending the
// WRONG JSON KEYS on the Runtime API response payload the whole time:
//
//  1. On success, the real, working wire key for the result payload is
//     the literal string "Result", NOT "ResultPayload" - this SDK's own
//     ResultPayload Go field (kept as-is; only its json tag changes)
//     was being silently ignored by the backend, which explains the
//     "GetDurableExecution.Result is never populated" finding
//     documented (before this fix) as an unconfirmed, non-SDK-fixable
//     backend limitation across every one of the conformance test
//     harness's 9 suites - it was never a backend limitation at all,
//     just this SDK sending the wrong field name.
//  2. On failure, the real, working wire shape is a SINGLE NESTED
//     "Error" object (Status: FAILED, Error: {ErrorMessage, ErrorType,
//     ErrorData, StackTrace}), NOT two flat top-level fields
//     ("ErrorMessage"/"ErrorType") as this SDK previously sent - the
//     backend was silently discarding both flat fields because neither
//     one is inside a recognized "Error" key, which explains the
//     separately-documented "ExecutionFailedDetails.Error.Payload comes
//     back completely empty for an uncaught callback/batch failure"
//     finding (previously investigated at length via two different,
//     both-inconclusive remediation attempts - see this type's own git
//     history for that investigation - neither attempt actually tried
//     nesting the error fields under a single "Error" key, which is the
//     real fix).
//
// Status's own three string values (SUCCEEDED/FAILED/PENDING) were
// already correct (confirmed both by this SDK's own prior real
// deployments, which always got ExecutionStatus right, and by the JS
// SDK's own InvocationStatus enum matching exactly) - only the
// success-payload and failure-payload field shapes were wrong.
type DurableExecutionOutput struct {
	Status ExecutionStatus `json:"Status"`

	// ResultPayload (Go field name kept for source/API compatibility with
	// every existing caller in this codebase - pkg/durable/testing's
	// runner.go/cloud_runner.go both already reference this exact field
	// name) is serialized on the wire as "Result", matching the real,
	// confirmed-working JS reference SDK's own
	// DurableExecutionInvocationOutputSucceeded.Result field exactly -
	// see this type's own top-level doc for the full bug writeup.
	ResultPayload *string `json:"Result,omitempty"`

	// Error carries BOTH ErrorMessage and ErrorType (and, in principle,
	// ErrorData/StackTrace, though this SDK does not currently populate
	// those) as a single nested object, matching the real, confirmed-
	// working JS reference SDK's own
	// DurableExecutionInvocationOutputFailed.Error field exactly (a
	// plain ErrorObject, the SAME wire shape already used everywhere
	// else in this package for a checkpointed operation failure's own
	// Error field - see ErrorObject's own doc) - see this type's own
	// top-level doc for the full bug writeup on why the previous flat
	// ErrorMessage/ErrorType fields never reached the backend correctly.
	Error *ErrorObject `json:"Error,omitempty"`
}

// ExecutionStatus reports the outcome of a single Lambda invocation within
// a durable execution's lifecycle. See DurableExecutionOutput's doc for
// the current confirmation status of these values.
type ExecutionStatus string

const (
	ExecutionStatusSucceeded ExecutionStatus = "SUCCEEDED"
	ExecutionStatusFailed    ExecutionStatus = "FAILED"
	ExecutionStatusPending   ExecutionStatus = "PENDING"
)
