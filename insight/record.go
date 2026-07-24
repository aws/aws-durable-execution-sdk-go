// Package insight provides observability record types for the AWS Durable
// Execution SDK. It defines the core schema for workflow insight records
// that capture execution snapshots for diagnostics and monitoring.
//
// This package is EXPERIMENTAL and may be changed or removed in future
// releases without a major-version bump.
package insight

import (
	"strings"
	"time"
)

// SchemaVersion is the fixed schema version for insight records.
const SchemaVersion = "1.0"

// RecordType is the discriminator value on every emitted record.
const RecordType = "WorkflowInsight"

// Status represents the execution-level status of a durable workflow.
type Status string

const (
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

// EmitMode controls when the insight plugin emits records.
//
// Note: B's reference defines EmitModeOnFailure (emit only on failure).
// This is intentionally deferred — use EmitOnComplete for terminal-only
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

// ContentField holds a content value that may have been truncated to fit
// size constraints.
type ContentField struct {
	Value     string `json:"value"`
	Truncated bool   `json:"truncated,omitempty"`
}

// OperationRecord describes a single durable operation within a Record.
type OperationRecord struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Type     string `json:"type"`
	SubType  string `json:"subType,omitempty"`
	Status   string `json:"status"`
	ParentID string `json:"parentId,omitempty"`

	StartTimestamp *time.Time `json:"startTimestamp,omitempty"`
	EndTimestamp   *time.Time `json:"endTimestamp,omitempty"`
	DurationMs     *int64     `json:"durationMs,omitempty"`

	Attempt int `json:"attempt,omitempty"`

	Result *ContentField `json:"result,omitempty"`
	Error  *ErrorRecord  `json:"error,omitempty"`
	Input  *ContentField `json:"input,omitempty"`

	// Truncated indicates this operation's result was dropped due to
	// size constraints (see Truncate).
	Truncated bool `json:"truncated,omitempty"`
}

// Record is the cumulative snapshot an insight plugin builds for a single
// durable execution. It is handed to each configured exporter on emission.
type Record struct {
	RecordType    string `json:"recordType"`
	SchemaVersion string `json:"schemaVersion"`

	ExecutionArn  string `json:"executionArn"`
	ExecutionName string `json:"executionName,omitempty"`
	FunctionName  string `json:"functionName,omitempty"`

	Status         Status     `json:"status"`
	StartTimestamp *time.Time `json:"startTimestamp,omitempty"`
	EndTimestamp   *time.Time `json:"endTimestamp,omitempty"`
	DurationMs     *int64     `json:"durationMs,omitempty"`

	InvocationCount int `json:"invocationCount,omitempty"`

	// EmittedAt is the time this record was created. Fixed at record
	// creation time (fixes B's bug where this was never set).
	EmittedAt time.Time `json:"emittedAt"`

	Operations []OperationRecord `json:"operations"`

	Error  *ErrorRecord  `json:"error,omitempty"`
	Input  *ContentField `json:"input,omitempty"`
	Output *ContentField `json:"output,omitempty"`

	Custom map[string]any `json:"custom,omitempty"`

	// Truncation metadata: set when the record was truncated to fit an
	// exporter's size limit (see Truncate).
	Truncated         bool `json:"truncated,omitempty"`
	DroppedOperations int  `json:"droppedOperations,omitempty"`
	DroppedInput      bool `json:"droppedInput,omitempty"`
	DroppedOutput     bool `json:"droppedOutput,omitempty"`
}

// NewRecord creates a Record with standard metadata pre-populated.
// EmittedAt is set to time.Now() (fixing B's bug where it was never set).
// ExecutionName is extracted from the ARN's last path segment (fixing B's
// bug where it was never populated).
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
// "arn:aws:lambda:us-east-1:123456789012:function:my-fn:5/durable-execution/exec-id/inv-id",
// it returns "exec-id". For simpler ARNs it returns the last segment
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
