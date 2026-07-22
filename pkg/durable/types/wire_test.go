package types

import (
	"encoding/json"
	"testing"
	"time"
)

// TestTime_UnmarshalsBothEncodings verifies types.Time accepts both
// timestamp encodings confirmed present on the real wire (see Time's doc
// comment): a unix-millis integer (used by
// DurableExecutionInvocationInput) and an ISO 8601 string (used by
// CheckpointDurableExecutionResponse). This regression-tests a real bug
// found during live integration testing against a deployed Lambda (see
// docs/checkpoint-replay-design.md).
func TestTime_UnmarshalsBothEncodings(t *testing.T) {
	t.Run("unix millis integer", func(t *testing.T) {
		var tm Time
		if err := json.Unmarshal([]byte(`1784236727585`), &tm); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		want := time.UnixMilli(1784236727585)
		if !tm.Time.Equal(want) {
			t.Errorf("got %v, want %v", tm.Time, want)
		}
	})

	t.Run("ISO 8601 string", func(t *testing.T) {
		var tm Time
		if err := json.Unmarshal([]byte(`"2026-07-16T21:19:08.358000+00:00"`), &tm); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		want := time.Date(2026, 7, 16, 21, 19, 8, 358000000, time.UTC)
		if !tm.Time.Equal(want) {
			t.Errorf("got %v, want %v", tm.Time, want)
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		var tm Time
		if err := json.Unmarshal([]byte(`{"not":"a timestamp"}`), &tm); err == nil {
			t.Error("expected an error for a non-timestamp JSON value")
		}
	})
}

// TestOperation_UnmarshalsRealCheckpointResponseShape verifies that
// Operation correctly parses the exact JSON shape returned by a live
// CheckpointDurableExecution call (captured via CloudWatch Logs during
// integration testing - see docs/checkpoint-replay-design.md), including
// the nested StepDetails and the ISO 8601 timestamp encoding.
func TestOperation_UnmarshalsRealCheckpointResponseShape(t *testing.T) {
	raw := `{
		"Id": "1",
		"Name": "create-greeting",
		"Type": "STEP",
		"StartTimestamp": "2026-07-16T21:19:08.358000+00:00",
		"EndTimestamp": "2026-07-16T21:19:08.358000+00:00",
		"Status": "SUCCEEDED",
		"StepDetails": {
			"Attempt": 1,
			"Result": "\"Hello, Go SDK v3!\""
		}
	}`

	var op Operation
	if err := json.Unmarshal([]byte(raw), &op); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if op.ID != "1" || op.Name != "create-greeting" || op.Type != OperationTypeStep {
		t.Errorf("unexpected identity fields: %+v", op)
	}
	if op.Status != OperationStatusSucceeded {
		t.Errorf("expected Succeeded, got %v", op.Status)
	}
	if op.StartTimestamp == nil {
		t.Fatal("expected StartTimestamp to be set")
	}
	if op.StepDetails == nil {
		t.Fatal("expected StepDetails to be set")
	}
	if op.StepDetails.Attempt != 1 {
		t.Errorf("expected Attempt=1, got %d", op.StepDetails.Attempt)
	}
	if op.StepDetails.Result == nil || *op.StepDetails.Result != `"Hello, Go SDK v3!"` {
		t.Errorf("unexpected Result: %v", op.StepDetails.Result)
	}
}
