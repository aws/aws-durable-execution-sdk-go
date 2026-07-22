package insight

import (
	"encoding/json"
	"testing"
)

func TestRenderRecord_ArrayFormat_KeepsOperationsOmitsByName(t *testing.T) {
	rec := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations:   []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}},
	}
	b, err := renderRecord(rec, OperationsFormatArray)
	if err != nil {
		t.Fatalf("renderRecord: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; !ok {
		t.Error("expected 'operations' key to be present")
	}
	if _, ok := fields["operationsByName"]; ok {
		t.Error("expected 'operationsByName' key to be absent for OperationsFormatArray")
	}
}

func TestRenderRecord_ByNameFormat_OmitsOperationsKeepsByName(t *testing.T) {
	rec := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations:   []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}},
	}
	b, err := renderRecord(rec, OperationsFormatByName)
	if err != nil {
		t.Fatalf("renderRecord: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' key to be ABSENT for OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' key to be present")
	}
}

func TestRenderRecord_BothFormat_IncludesBoth(t *testing.T) {
	rec := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations:   []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}},
	}
	b, err := renderRecord(rec, OperationsFormatBoth)
	if err != nil {
		t.Fatalf("renderRecord: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; !ok {
		t.Error("expected 'operations' key to be present for OperationsFormatBoth")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' key to be present for OperationsFormatBoth")
	}
}

func TestRenderRecord_EmptyFormat_DefaultsToArray(t *testing.T) {
	rec := WorkflowInsightRecord{ExecutionARN: "arn:test"}
	b, err := renderRecord(rec, "")
	if err != nil {
		t.Fatalf("renderRecord: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; !ok {
		t.Error("expected the empty-format default to behave like OperationsFormatArray")
	}
}

func TestRenderRecord_OtherFieldsPreserved(t *testing.T) {
	rec := WorkflowInsightRecord{
		RecordType: RecordType, SchemaVersion: SchemaVersion,
		ExecutionARN: "arn:test", Status: ExecutionStatusSucceeded,
	}
	b, err := renderRecord(rec, OperationsFormatByName)
	if err != nil {
		t.Fatalf("renderRecord: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded["recordType"] != "WorkflowInsight" || decoded["status"] != "SUCCEEDED" {
		t.Errorf("expected every other field to survive rendering unchanged, got %+v", decoded)
	}
}
