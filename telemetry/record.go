// Package telemetry provides observability record types for the AWS Durable
// Execution SDK. It defines the core schema for durable execution
// telemetry records that capture execution snapshots for diagnostics and
// monitoring.
//
// This package is EXPERIMENTAL. It is built on the experimental plugin API
// of the durable package, so the telemetry module stays at major version 0.
// Any release may change this package's API and its record schema.
package telemetry

import (
	"encoding/json"
	"strings"
	"time"
)

// SchemaVersion is the fixed schema version for telemetry records.
const SchemaVersion = "1.0"

// RecordType is the discriminator value on every emitted record.
const RecordType = "DurableExecutionTelemetry"

// Status represents the execution-level status of a durable workflow.
type Status string

const (
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

// EmitMode controls when the telemetry plugin emits records.
//
// There is no failure-only mode: use EmitOnComplete for terminal-only
// emission and filter downstream if needed.
type EmitMode string

const (
	// EmitAlways emits a record after every invocation regardless of
	// outcome.
	EmitAlways EmitMode = "always"

	// EmitOnChange emits a running snapshot on every operation start/end
	// in addition to the terminal record.
	EmitOnChange EmitMode = "on-change"

	// EmitOnComplete emits a record only when the execution reaches a
	// terminal state (succeeded or failed).
	EmitOnComplete EmitMode = "on-complete"
)

// ErrorRecord carries an error's type and message.
type ErrorRecord struct {
	Type       string `json:"type"`
	Message    string `json:"message"`
	StackTrace string `json:"stackTrace,omitempty"`
}

// OperationRecord describes a single durable operation within a Record.
type OperationRecord struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Type     string `json:"type"`
	SubType  string `json:"subType,omitempty"`
	Status   string `json:"status"`
	ParentID string `json:"parentId,omitempty"`

	// StartTimestamp and EndTimestamp serialize as the JSON keys
	// startTime and endTime. See Record.MarshalJSON for the format.
	StartTimestamp *time.Time `json:"startTime,omitempty"`
	EndTimestamp   *time.Time `json:"endTime,omitempty"`
	DurationMs     *int64     `json:"durationMs,omitempty"`

	Attempt int `json:"attempt,omitempty"`

	// Result is the operation's result as a JSON value. The checkpointed
	// wire text is used as-is when it is valid JSON; otherwise it is
	// encoded as a JSON string. See ContentConfig.
	Result json.RawMessage `json:"result,omitempty"`
	Error  *ErrorRecord    `json:"error,omitempty"`

	// Truncated indicates this operation's result was dropped due to
	// size constraints (see Truncate).
	Truncated bool `json:"truncated,omitempty"`
}

// Record is the cumulative snapshot a telemetry plugin builds for a single
// durable execution. It is handed to each configured exporter on emission.
type Record struct {
	RecordType    string `json:"recordType"`
	SchemaVersion string `json:"schemaVersion"`

	ExecutionArn  string `json:"executionArn"`
	ExecutionName string `json:"executionName,omitempty"`

	// FunctionName, FunctionQualifier, Region, and AccountID are parsed
	// from the execution ARN. They are always emitted, empty when the ARN
	// does not carry them, so every record has the same identity keys.
	FunctionName      string `json:"functionName"`
	FunctionQualifier string `json:"functionQualifier"`
	Region            string `json:"region"`
	AccountID         string `json:"accountId"`

	Status Status `json:"status"`

	// StartTimestamp and EndTimestamp serialize as the JSON keys
	// startTime and endTime. See MarshalJSON for the format.
	StartTimestamp *time.Time `json:"startTime,omitempty"`
	EndTimestamp   *time.Time `json:"endTime,omitempty"`
	DurationMs     *int64     `json:"durationMs,omitempty"`

	// EmittedAt is the time this record was created, refreshed to the
	// emission time when a snapshot is sent.
	EmittedAt time.Time `json:"emittedAt"`

	Operations []OperationRecord `json:"operations"`

	Error *ErrorRecord `json:"error,omitempty"`

	// Input and Output are the execution input and result as JSON values,
	// marshaled once from the handler's typed values. A Go string is a
	// JSON string. See ContentConfig.
	Input  json.RawMessage `json:"input,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`

	Custom map[string]any `json:"custom,omitempty"`

	// Truncation metadata: set when the record was truncated to fit an
	// exporter's size limit (see Truncate).
	Truncated         bool `json:"truncated,omitempty"`
	DroppedOperations int  `json:"droppedOperations,omitempty"`
	DroppedInput      bool `json:"droppedInput,omitempty"`
	DroppedOutput     bool `json:"droppedOutput,omitempty"`
}

// timestampLayout is the serialized form of every timestamp in a record:
// UTC, exactly three fraction digits, and a literal Z. time.RFC3339Nano
// trims trailing zeros, so its digit count varies; this layout does not.
const timestampLayout = "2006-01-02T15:04:05.000Z"

// formatTimestamp renders t in timestampLayout. Non-UTC times are
// converted to UTC first, so the Z suffix is always correct.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(timestampLayout)
}

// formatOptionalTimestamp renders t, or returns nil when t is nil, so
// omitempty drops the key.
func formatOptionalTimestamp(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := formatTimestamp(*t)
	return &s
}

// MarshalJSON writes every timestamp in timestampLayout. encoding/json
// would otherwise call time.Time.MarshalJSON, which uses RFC3339Nano.
//
// The method marshals an alias of Record with shadow string fields that
// carry the same JSON keys as the time.Time fields. encoding/json keeps
// the shallower field when two fields share a key, so the shadow strings
// replace the time values in the output. The Go field types stay
// time.Time, and json.Unmarshal into Record still works because time.Time
// parses the three-digit form.
func (r Record) MarshalJSON() ([]byte, error) {
	type alias Record
	return json.Marshal(struct {
		alias
		StartTime *string `json:"startTime,omitempty"`
		EndTime   *string `json:"endTime,omitempty"`
		EmittedAt string  `json:"emittedAt"`
	}{
		alias:     alias(r),
		StartTime: formatOptionalTimestamp(r.StartTimestamp),
		EndTime:   formatOptionalTimestamp(r.EndTimestamp),
		EmittedAt: formatTimestamp(r.EmittedAt),
	})
}

// MarshalJSON writes the operation timestamps in timestampLayout. See
// Record.MarshalJSON for the mechanism.
func (o OperationRecord) MarshalJSON() ([]byte, error) {
	type alias OperationRecord
	return json.Marshal(struct {
		alias
		StartTime *string `json:"startTime,omitempty"`
		EndTime   *string `json:"endTime,omitempty"`
	}{
		alias:     alias(o),
		StartTime: formatOptionalTimestamp(o.StartTimestamp),
		EndTime:   formatOptionalTimestamp(o.EndTimestamp),
	})
}

// NewRecord creates a Record with standard metadata pre-populated.
// EmittedAt is set to time.Now() and ExecutionName is extracted from the
// ARN.
func NewRecord(executionArn string) *Record {
	return &Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  executionArn,
		ExecutionName: extractExecutionName(executionArn),
		EmittedAt:     time.Now(),
		Operations:    []OperationRecord{},
	}
}

// extractExecutionName extracts the execution name from an ARN. For a
// durable-execution ARN like
// "arn:aws:lambda:us-east-1:123456789012:function:my-fn:5/durable-execution/execution-name/execution-id",
// it returns "execution-name". For simpler ARNs it returns the last segment
// after '/'.
func extractExecutionName(arn string) string {
	// Look for /durable-execution/{name}/ pattern first.
	const marker = "/durable-execution/"
	if idx := strings.Index(arn, marker); idx >= 0 {
		rest := arn[idx+len(marker):]
		if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
			return rest[:slashIdx]
		}
		return rest
	}

	// Fall back to the last segment after '/'.
	if lastSlash := strings.LastIndex(arn, "/"); lastSlash >= 0 {
		return arn[lastSlash+1:]
	}
	return ""
}
