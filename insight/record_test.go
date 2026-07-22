package insight

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWorkflowInsightRecord_JSONShape(t *testing.T) {
	now := time.Date(2026, 6, 16, 17, 0, 22, 0, time.UTC)
	end := now.Add(5 * time.Second)
	dur := int64(5000)
	rec := WorkflowInsightRecord{
		RecordType:        RecordType,
		SchemaVersion:     SchemaVersion,
		EmittedAt:         end,
		ExecutionARN:      "arn:aws:lambda:us-east-1:123456789012:function:order-processor:5",
		FunctionName:      "order-processor",
		FunctionQualifier: "5",
		Region:            "us-east-1",
		AccountID:         "123456789012",
		Status:            ExecutionStatusSucceeded,
		StartTime:         now,
		EndTime:           &end,
		DurationMs:        &dur,
		Input:             map[string]any{"orderId": "order-12345"},
		Output:            map[string]any{"status": "completed"},
		Operations: []OperationRecord{
			{ID: "abc123", Name: "validate-order", Type: "STEP", SubType: "Step", Status: "SUCCEEDED"},
		},
	}

	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded["recordType"] != "WorkflowInsight" {
		t.Errorf("expected recordType=WorkflowInsight, got %v", decoded["recordType"])
	}
	if decoded["schemaVersion"] != "1.0" {
		t.Errorf("expected schemaVersion=1.0, got %v", decoded["schemaVersion"])
	}
	if decoded["status"] != "SUCCEEDED" {
		t.Errorf("expected status=SUCCEEDED, got %v", decoded["status"])
	}
	if decoded["functionName"] != "order-processor" {
		t.Errorf("expected functionName=order-processor, got %v", decoded["functionName"])
	}
	ops, ok := decoded["operations"].([]any)
	if !ok || len(ops) != 1 {
		t.Fatalf("expected operations array of length 1, got %v", decoded["operations"])
	}
}

func TestWorkflowInsightRecord_OmitsEmptyOptionalFields(t *testing.T) {
	rec := WorkflowInsightRecord{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionARN:  "arn:aws:lambda:us-east-1:123456789012:function:f:1",
		Status:        ExecutionStatusRunning,
		StartTime:     time.Now(),
		Operations:    nil,
	}

	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, field := range []string{"endTime", "durationMs", "input", "output", "error", "executionName"} {
		if _, present := decoded[field]; present {
			t.Errorf("expected field %q to be omitted for a RUNNING record with no value, but it was present: %v", field, decoded[field])
		}
	}
	// operations is NOT omitempty (always present, even as an empty
	// array) - matching the JS SDK's own record schema, which always
	// includes operations: OperationRecord[] (never omits the key
	// entirely, even for zero operations).
	if _, present := decoded["operations"]; !present {
		t.Error("expected operations field to always be present, even when empty")
	}
}

func TestParseFunctionARN(t *testing.T) {
	tests := []struct {
		arn                                        string
		region, accountID, functionName, qualifier string
	}{
		{
			arn:          "arn:aws:lambda:us-east-1:123456789012:function:order-processor:5",
			region:       "us-east-1",
			accountID:    "123456789012",
			functionName: "order-processor",
			qualifier:    "5",
		},
		{
			arn:          "arn:aws:lambda:us-west-2:987654321098:function:my-fn",
			region:       "us-west-2",
			accountID:    "987654321098",
			functionName: "my-fn",
			qualifier:    "",
		},
		{
			// Real durable-execution ARN extended form (confirmed
			// present on the wire per this repo's own
			// docs/checkpoint-replay-design.md): function ARN followed
			// by /durable-execution/{executionId}/{invocationId}.
			arn:          "arn:aws:lambda:us-east-1:730758745077:function:simple-step-go-example:5/durable-execution/abc123/inv456",
			region:       "us-east-1",
			accountID:    "730758745077",
			functionName: "simple-step-go-example",
			qualifier:    "5",
		},
		{
			arn:          "not-an-arn",
			region:       "",
			accountID:    "",
			functionName: "",
			qualifier:    "",
		},
	}

	for _, tt := range tests {
		region, accountID, functionName, qualifier := parseFunctionARN(tt.arn)
		if region != tt.region || accountID != tt.accountID || functionName != tt.functionName || qualifier != tt.qualifier {
			t.Errorf("parseFunctionARN(%q) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
				tt.arn, region, accountID, functionName, qualifier,
				tt.region, tt.accountID, tt.functionName, tt.qualifier)
		}
	}
}
