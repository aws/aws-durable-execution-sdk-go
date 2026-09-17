// SPDX-License-Identifier: Apache-2.0

// Package wire defines the JSON shapes exchanged between a durable
// function invocation and the durable execution service: the invocation
// input with its embedded operation log page, and the invocation
// response.
//
// The durable package decodes these shapes on every invocation and the
// durabletest package encodes them to drive a handler locally. Both
// packages import this one definition so the two sides cannot drift.
//
// This package is internal to the module and is not part of the SDK API.
package wire

import (
	"encoding/json"
	"strconv"
	"time"
)

// Timestamp is a checkpoint timestamp that may arrive as either an
// RFC3339 string or a numeric epoch-milliseconds value. Valid is false
// when the field was absent, null, or unparseable.
type Timestamp struct {
	Time  time.Time
	Valid bool
}

// MarshalJSON encodes a valid Timestamp as an RFC3339 string and an
// invalid one as null.
func (ts Timestamp) MarshalJSON() ([]byte, error) {
	if !ts.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ts.Time.Format(time.RFC3339Nano))
}

// UnmarshalJSON accepts null, an RFC3339 string, or an epoch-milliseconds
// number. Any other value leaves the Timestamp invalid without error, so
// an unexpected timestamp encoding never rejects the whole payload.
func (ts *Timestamp) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	// Try as string first (RFC3339).
	var s string
	if err := json.Unmarshal(data, &s); err == nil && s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			ts.Time = t
			ts.Valid = true
		}
		return nil
	}
	// Try as number (epoch milliseconds).
	var n json.Number
	if err := json.Unmarshal(data, &n); err == nil {
		if ms, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
			ts.Time = time.UnixMilli(ms)
			ts.Valid = true
		}
	}
	return nil
}

// InvocationInput is the payload delivered to a durable function
// invocation. It carries the execution identity, the checkpoint token for
// this invocation cycle, and the first page of the checkpointed operation
// log.
//
// The customer event is not a separate field: it travels as the serialized
// InputPayload of the execution operation inside the initial state.
type InvocationInput struct {
	DurableExecutionArn   string                `json:"DurableExecutionArn"`
	CheckpointToken       string                `json:"CheckpointToken"`
	UpdatedOperationIds   []string              `json:"UpdatedOperationIds,omitempty"`
	InitialExecutionState InitialExecutionState `json:"InitialExecutionState"`
}

// InitialExecutionState is the operation log page embedded in the
// invocation payload. Further pages are fetched with
// GetDurableExecutionState using NextMarker.
type InitialExecutionState struct {
	Operations []Operation `json:"Operations"`
	NextMarker string      `json:"NextMarker,omitempty"`
}

// Operation is the JSON shape of one checkpointed operation in the
// invocation payload, narrowed to the fields the engine consumes. Exactly
// one of the *Details fields is populated, selected by Type.
type Operation struct {
	Id                   string                `json:"Id"`
	ParentId             string                `json:"ParentId,omitempty"`
	Status               string                `json:"Status"`
	Type                 string                `json:"Type,omitempty"`
	SubType              string                `json:"SubType,omitempty"`
	Name                 string                `json:"Name,omitempty"`
	StartTimestamp       Timestamp             `json:"StartTimestamp,omitempty"`
	EndTimestamp         Timestamp             `json:"EndTimestamp,omitempty"`
	ExecutionDetails     *ExecutionDetails     `json:"ExecutionDetails,omitempty"`
	StepDetails          *StepDetails          `json:"StepDetails,omitempty"`
	ChainedInvokeDetails *ChainedInvokeDetails `json:"ChainedInvokeDetails,omitempty"`
	ContextDetails       *ContextDetails       `json:"ContextDetails,omitempty"`
	CallbackDetails      *CallbackDetails      `json:"CallbackDetails,omitempty"`
}

// ExecutionDetails carries the execution operation's payload fields.
type ExecutionDetails struct {
	// InputPayload is the serialized customer event for the execution.
	InputPayload string `json:"InputPayload,omitempty"`
}

// StepDetails carries a step operation's checkpointed attempt state.
// NextAttemptTimestamp is deliberately not decoded: the engine never
// consumes it (the service owns the retry timer), and its numeric wire
// encoding does not fit time.Time.
type StepDetails struct {
	Attempt int          `json:"Attempt,omitempty"`
	Result  string       `json:"Result,omitempty"`
	Error   *ErrorObject `json:"Error,omitempty"`
}

// ChainedInvokeDetails carries a chained invoke's checkpointed outcome.
type ChainedInvokeDetails struct {
	Result string       `json:"Result,omitempty"`
	Error  *ErrorObject `json:"Error,omitempty"`
}

// ContextDetails carries a child context's checkpointed outcome.
type ContextDetails struct {
	Result         string       `json:"Result,omitempty"`
	ReplayChildren bool         `json:"ReplayChildren,omitempty"`
	Error          *ErrorObject `json:"Error,omitempty"`
}

// CallbackDetails carries a callback operation's checkpointed outcome.
type CallbackDetails struct {
	CallbackId string       `json:"CallbackId,omitempty"`
	Result     string       `json:"Result,omitempty"`
	Error      *ErrorObject `json:"Error,omitempty"`
}

// ErrorObject is a recorded failure. It appears inside the *Details of a
// failed operation and as the Error of a FAILED invocation response. Every
// field is optional on the wire; an empty field is omitted when encoding.
type ErrorObject struct {
	ErrorType    string `json:"ErrorType,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
	ErrorData    string `json:"ErrorData,omitempty"`
	StackTrace   string `json:"StackTrace,omitempty"`
}

// InvocationResponse is what a durable invocation returns: the terminal
// result, a failure, or PENDING when the execution suspends to resume in a
// later invocation. Result is a pre-serialized JSON string, so it is
// double-encoded on the wire.
type InvocationResponse struct {
	Status string       `json:"Status"`
	Result *string      `json:"Result,omitempty"`
	Error  *ErrorObject `json:"Error,omitempty"`
}

// Invocation statuses carried in InvocationResponse.Status.
const (
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusPending   = "PENDING"
)
