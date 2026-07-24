package insight

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRenderRecord_DefaultFormat(t *testing.T) {
	r := Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:test",
		Status:        StatusSucceeded,
		EmittedAt:     time.Now(),
		Operations: []OperationRecord{
			{ID: "1", Name: "step-a", Type: "STEP", Status: "SUCCEEDED"},
		},
	}

	b, err := RenderRecord(r, "")
	if err != nil {
		t.Fatalf("RenderRecord failed: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if _, ok := decoded["operations"]; !ok {
		t.Error("default format should include operations")
	}
	if _, ok := decoded["operationsByName"]; ok {
		t.Error("default format should not include operationsByName")
	}
}

func TestRenderRecord_ArrayFormat(t *testing.T) {
	r := Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:test",
		Status:        StatusSucceeded,
		EmittedAt:     time.Now(),
		Operations: []OperationRecord{
			{ID: "1", Name: "step-a", Type: "STEP", Status: "SUCCEEDED"},
		},
	}

	b, err := RenderRecord(r, OperationsFormatArray)
	if err != nil {
		t.Fatalf("RenderRecord failed: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if _, ok := decoded["operations"]; !ok {
		t.Error("array format should include operations")
	}
	if _, ok := decoded["operationsByName"]; ok {
		t.Error("array format should not include operationsByName")
	}
}

func TestRenderRecord_ByNameFormat(t *testing.T) {
	r := Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:test",
		Status:        StatusSucceeded,
		EmittedAt:     time.Now(),
		Operations: []OperationRecord{
			{ID: "1", Name: "step-a", Type: "STEP", Status: "SUCCEEDED"},
		},
	}

	b, err := RenderRecord(r, OperationsFormatByName)
	if err != nil {
		t.Fatalf("RenderRecord failed: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if _, ok := decoded["operations"]; ok {
		t.Error("by-name format should not include operations array")
	}
	if _, ok := decoded["operationsByName"]; !ok {
		t.Error("by-name format should include operationsByName")
	}

	// Verify the by-name content is correct.
	var byName map[string]OperationSummary
	if err := json.Unmarshal(decoded["operationsByName"], &byName); err != nil {
		t.Fatalf("Unmarshal operationsByName: %v", err)
	}
	if _, ok := byName["step-a"]; !ok {
		t.Error("operationsByName should contain step-a")
	}
}

func TestRenderRecord_BothFormat(t *testing.T) {
	r := Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:test",
		Status:        StatusSucceeded,
		EmittedAt:     time.Now(),
		Operations: []OperationRecord{
			{ID: "1", Name: "step-a", Type: "STEP", Status: "SUCCEEDED"},
		},
	}

	b, err := RenderRecord(r, OperationsFormatBoth)
	if err != nil {
		t.Fatalf("RenderRecord failed: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if _, ok := decoded["operations"]; !ok {
		t.Error("both format should include operations array")
	}
	if _, ok := decoded["operationsByName"]; !ok {
		t.Error("both format should include operationsByName")
	}
}

func TestRenderRecord_EmptyOperations(t *testing.T) {
	r := Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:test",
		Status:        StatusRunning,
		EmittedAt:     time.Now(),
		Operations:    []OperationRecord{},
	}

	b, err := RenderRecord(r, OperationsFormatByName)
	if err != nil {
		t.Fatalf("RenderRecord failed: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if _, ok := decoded["operationsByName"]; !ok {
		t.Error("by-name format should include operationsByName even when empty")
	}
}
