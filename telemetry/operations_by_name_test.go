package telemetry

import (
	"encoding/json"
	"strings"
	"testing"
)

// checkDuration asserts a duration pointer is non-nil and carries want.
func checkDuration(t *testing.T, field string, got *int64, want int64) {
	t.Helper()
	if got == nil {
		t.Errorf("%s = nil, want %d", field, want)
		return
	}
	if *got != want {
		t.Errorf("%s = %d, want %d", field, *got, want)
	}
}

func TestGroupOperationsByName_Empty(t *testing.T) {
	result := GroupOperationsByName(nil)
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

func TestGroupOperationsByName_SkipsUnnamed(t *testing.T) {
	ops := []OperationRecord{
		{ID: "1", Name: "", Type: "STEP", Status: "SUCCEEDED"},
		{ID: "2", Name: "named", Type: "STEP", Status: "SUCCEEDED"},
	}
	result := GroupOperationsByName(ops)

	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
	if _, ok := result["named"]; !ok {
		t.Error("expected 'named' key")
	}
}

func TestGroupOperationsByName_SingleOccurrence(t *testing.T) {
	dur := int64(500)
	ops := []OperationRecord{
		{
			ID:         "1",
			Name:       "validate",
			Type:       "STEP",
			SubType:    "Step",
			Status:     "SUCCEEDED",
			DurationMs: &dur,
			Attempt:    1,
			Error:      &ErrorRecord{Type: "TestErr", Message: "oops"},
			Result:     &ContentField{Value: "result-data"},
		},
	}
	result := GroupOperationsByName(ops)

	agg, ok := result["validate"]
	if !ok {
		t.Fatal("expected 'validate' key")
	}
	if agg.Count != 1 {
		t.Errorf("Count = %d, want 1", agg.Count)
	}
	if agg.Type != "STEP" {
		t.Errorf("Type = %q, want STEP", agg.Type)
	}
	if agg.SubType != "Step" {
		t.Errorf("SubType = %q, want Step", agg.SubType)
	}
	if agg.Status != "SUCCEEDED" {
		t.Errorf("Status = %q, want SUCCEEDED", agg.Status)
	}
	checkDuration(t, "MinDurationMs", agg.MinDurationMs, 500)
	checkDuration(t, "MaxDurationMs", agg.MaxDurationMs, 500)
	checkDuration(t, "TotalDurationMs", agg.TotalDurationMs, 500)
	if agg.MaxAttempt != 1 {
		t.Errorf("MaxAttempt = %d, want 1", agg.MaxAttempt)
	}
	// Error and Result should be populated for single occurrence.
	if agg.Error == nil {
		t.Error("Error should be populated for single occurrence")
	}
	if agg.Result == nil {
		t.Error("Result should be populated for single occurrence")
	}
}

func TestGroupOperationsByName_MultipleOccurrences(t *testing.T) {
	dur1 := int64(100)
	dur2 := int64(300)
	dur3 := int64(200)
	ops := []OperationRecord{
		{ID: "1", Name: "process", Type: "STEP", Status: "FAILED", DurationMs: &dur1, Attempt: 1, Error: &ErrorRecord{Type: "E", Message: "m"}},
		{ID: "2", Name: "process", Type: "STEP", SubType: "Retry", Status: "FAILED", DurationMs: &dur2, Attempt: 2},
		{ID: "3", Name: "process", Type: "STEP", SubType: "Retry", Status: "SUCCEEDED", DurationMs: &dur3, Attempt: 3},
	}
	result := GroupOperationsByName(ops)

	agg, ok := result["process"]
	if !ok {
		t.Fatal("expected 'process' key")
	}
	if agg.Count != 3 {
		t.Errorf("Count = %d, want 3", agg.Count)
	}
	if agg.FailedCount != 2 {
		t.Errorf("FailedCount = %d, want 2", agg.FailedCount)
	}
	if agg.MaxAttempt != 3 {
		t.Errorf("MaxAttempt = %d, want 3", agg.MaxAttempt)
	}
	checkDuration(t, "MinDurationMs", agg.MinDurationMs, 100)
	checkDuration(t, "MaxDurationMs", agg.MaxDurationMs, 300)
	checkDuration(t, "TotalDurationMs", agg.TotalDurationMs, 600)
	// Status should be the LAST occurrence's status.
	if agg.Status != "SUCCEEDED" {
		t.Errorf("Status = %q, want SUCCEEDED (last occurrence)", agg.Status)
	}
	// SubType should be the LAST occurrence's subtype.
	if agg.SubType != "Retry" {
		t.Errorf("SubType = %q, want Retry (last occurrence)", agg.SubType)
	}
	// Error and Result should NOT be populated for multiple occurrences.
	if agg.Error != nil {
		t.Error("Error should be nil for multiple occurrences")
	}
	if agg.Result != nil {
		t.Error("Result should be nil for multiple occurrences")
	}
}

func TestGroupOperationsByName_FirstOccurrenceNilDuration(t *testing.T) {
	// The first occurrence of a name has no DurationMs (e.g. an in-progress
	// operation captured in a mid-execution snapshot), a later occurrence
	// does. MinDurationMs must reflect the only timed occurrence, not stay
	// at the zero value.
	dur := int64(300)
	ops := []OperationRecord{
		{ID: "1", Name: "fetch", Type: "STEP", Status: "STARTED"},
		{ID: "2", Name: "fetch", Type: "STEP", Status: "SUCCEEDED", DurationMs: &dur},
	}
	result := GroupOperationsByName(ops)

	agg, ok := result["fetch"]
	if !ok {
		t.Fatal("expected 'fetch' key")
	}
	if agg.Count != 2 {
		t.Errorf("Count = %d, want 2", agg.Count)
	}
	checkDuration(t, "MinDurationMs", agg.MinDurationMs, 300)
	checkDuration(t, "MaxDurationMs", agg.MaxDurationMs, 300)
	checkDuration(t, "TotalDurationMs", agg.TotalDurationMs, 300)
}

// TestGroupOperationsByName_ZeroDurationSerializes pins the wire distinction
// between "no duration was recorded" (duration fields omitted from the JSON)
// and "the recorded duration was 0 ms" (fields serialize as 0), matching the
// JavaScript implementation.
func TestGroupOperationsByName_ZeroDurationSerializes(t *testing.T) {
	zero := int64(0)
	result := GroupOperationsByName([]OperationRecord{
		{ID: "1", Name: "instant", Type: "STEP", Status: "SUCCEEDED", DurationMs: &zero},
		{ID: "2", Name: "untimed", Type: "STEP", Status: "STARTED"},
	})

	instant, err := json.Marshal(result["instant"])
	if err != nil {
		t.Fatalf("marshal instant summary: %v", err)
	}
	for _, want := range []string{`"minDurationMs":0`, `"maxDurationMs":0`, `"totalDurationMs":0`} {
		if !strings.Contains(string(instant), want) {
			t.Errorf("summary with a recorded 0 ms duration lacks %s: %s", want, instant)
		}
	}

	untimed, err := json.Marshal(result["untimed"])
	if err != nil {
		t.Fatalf("marshal untimed summary: %v", err)
	}
	if strings.Contains(string(untimed), "DurationMs") {
		t.Errorf("summary with no recorded duration serializes duration fields: %s", untimed)
	}
}

func TestGroupOperationsByName_MixedNames(t *testing.T) {
	dur := int64(50)
	ops := []OperationRecord{
		{ID: "1", Name: "step-a", Type: "STEP", Status: "SUCCEEDED", DurationMs: &dur},
		{ID: "2", Name: "step-b", Type: "STEP", Status: "FAILED", DurationMs: &dur},
		{ID: "3", Name: "step-a", Type: "STEP", Status: "SUCCEEDED", DurationMs: &dur},
	}
	result := GroupOperationsByName(ops)

	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if result["step-a"].Count != 2 {
		t.Errorf("step-a Count = %d, want 2", result["step-a"].Count)
	}
	if result["step-b"].Count != 1 {
		t.Errorf("step-b Count = %d, want 1", result["step-b"].Count)
	}
	if result["step-b"].FailedCount != 1 {
		t.Errorf("step-b FailedCount = %d, want 1", result["step-b"].FailedCount)
	}
}
