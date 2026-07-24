package insight

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewRecord_SetsEmittedAt(t *testing.T) {
	before := time.Now()
	r := NewRecord("arn:aws:lambda:us-east-1:123456789012:function:my-fn:1")
	after := time.Now()

	if r.EmittedAt.Before(before) || r.EmittedAt.After(after) {
		t.Errorf("EmittedAt should be between %v and %v, got %v", before, after, r.EmittedAt)
	}
}

func TestNewRecord_SetsMetadata(t *testing.T) {
	r := NewRecord("arn:aws:lambda:us-east-1:123456789012:function:my-fn:1")

	if r.RecordType != RecordType {
		t.Errorf("RecordType = %q, want %q", r.RecordType, RecordType)
	}
	if r.SchemaVersion != SchemaVersion {
		t.Errorf("SchemaVersion = %q, want %q", r.SchemaVersion, SchemaVersion)
	}
	if r.Operations == nil {
		t.Error("Operations should be initialized, got nil")
	}
}

func TestNewRecord_ExtractsExecutionName(t *testing.T) {
	tests := []struct {
		name     string
		arn      string
		wantName string
	}{
		{
			name:     "durable execution ARN",
			arn:      "arn:aws:lambda:us-east-1:123456789012:function:my-fn:5/durable-execution/exec-abc/inv-123",
			wantName: "exec-abc",
		},
		{
			name:     "durable execution ARN without invocation",
			arn:      "arn:aws:lambda:us-east-1:123456789012:function:my-fn:5/durable-execution/exec-xyz",
			wantName: "exec-xyz",
		},
		{
			name:     "simple function ARN with qualifier",
			arn:      "arn:aws:lambda:us-east-1:123456789012:function:my-fn:5",
			wantName: "",
		},
		{
			name:     "ARN with path",
			arn:      "arn:aws:states:us-east-1:123456789012:execution:my-state-machine/my-execution",
			wantName: "my-execution",
		},
		{
			name:     "no slash",
			arn:      "simple-string",
			wantName: "",
		},
		{
			name:     "empty string",
			arn:      "",
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractExecutionName(tt.arn)
			if got != tt.wantName {
				t.Errorf("extractExecutionName(%q) = %q, want %q", tt.arn, got, tt.wantName)
			}
		})
	}
}

func TestRecord_JSONShape(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	end := now.Add(5 * time.Second)
	dur := int64(5000)

	r := &Record{
		RecordType:     RecordType,
		SchemaVersion:  SchemaVersion,
		ExecutionArn:   "arn:aws:lambda:us-east-1:123456789012:function:order-processor:5",
		ExecutionName:  "exec-123",
		FunctionName:   "order-processor",
		Status:         StatusSucceeded,
		StartTimestamp: &now,
		EndTimestamp:   &end,
		DurationMs:     &dur,
		EmittedAt:      end,
		Input:          &ContentField{Value: `{"orderId":"12345"}`},
		Output:         &ContentField{Value: `{"status":"done"}`},
		Operations: []OperationRecord{
			{ID: "op1", Name: "validate", Type: "STEP", Status: "SUCCEEDED"},
		},
	}

	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded["recordType"] != "WorkflowInsight" {
		t.Errorf("recordType = %v, want WorkflowInsight", decoded["recordType"])
	}
	if decoded["schemaVersion"] != "1.0" {
		t.Errorf("schemaVersion = %v, want 1.0", decoded["schemaVersion"])
	}
	if decoded["status"] != "SUCCEEDED" {
		t.Errorf("status = %v, want SUCCEEDED", decoded["status"])
	}
	if decoded["executionName"] != "exec-123" {
		t.Errorf("executionName = %v, want exec-123", decoded["executionName"])
	}
}

func TestRecord_OmitsEmptyOptionalFields(t *testing.T) {
	r := &Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:test",
		Status:        StatusRunning,
		EmittedAt:     time.Now(),
		Operations:    []OperationRecord{},
	}

	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	optionals := []string{
		"endTimestamp", "durationMs", "input", "output", "error",
		"executionName", "functionName", "custom",
	}
	for _, field := range optionals {
		if _, present := decoded[field]; present {
			t.Errorf("field %q should be omitted when empty, but was present", field)
		}
	}

	// operations should always be present (even empty).
	if _, present := decoded["operations"]; !present {
		t.Error("operations should always be present, even when empty")
	}
}

func TestOperationRecord_DurationMs(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(3500 * time.Millisecond)
	dur := int64(3500)

	op := OperationRecord{
		ID:             "op1",
		Name:           "my-step",
		Type:           "STEP",
		Status:         "SUCCEEDED",
		StartTimestamp: &start,
		EndTimestamp:   &end,
		DurationMs:     &dur,
	}

	b, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded["durationMs"] != float64(3500) {
		t.Errorf("durationMs = %v, want 3500", decoded["durationMs"])
	}
}

func TestStatus_Values(t *testing.T) {
	tests := []struct {
		status Status
		want   string
	}{
		{StatusRunning, "RUNNING"},
		{StatusSucceeded, "SUCCEEDED"},
		{StatusFailed, "FAILED"},
	}

	for _, tt := range tests {
		if string(tt.status) != tt.want {
			t.Errorf("Status %v = %q, want %q", tt.status, string(tt.status), tt.want)
		}
	}
}

func TestEmitMode_Values(t *testing.T) {
	tests := []struct {
		mode EmitMode
		want string
	}{
		{EmitAlways, "always"},
		{EmitOnChange, "on-change"},
		{EmitOnComplete, "on-complete"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.want {
			t.Errorf("EmitMode %v = %q, want %q", tt.mode, string(tt.mode), tt.want)
		}
	}
}
