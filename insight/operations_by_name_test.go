package insight

import "testing"

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
	if agg.MinDurationMs != 500 {
		t.Errorf("MinDurationMs = %d, want 500", agg.MinDurationMs)
	}
	if agg.MaxDurationMs != 500 {
		t.Errorf("MaxDurationMs = %d, want 500", agg.MaxDurationMs)
	}
	if agg.TotalDurationMs != 500 {
		t.Errorf("TotalDurationMs = %d, want 500", agg.TotalDurationMs)
	}
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
	if agg.MinDurationMs != 100 {
		t.Errorf("MinDurationMs = %d, want 100", agg.MinDurationMs)
	}
	if agg.MaxDurationMs != 300 {
		t.Errorf("MaxDurationMs = %d, want 300", agg.MaxDurationMs)
	}
	if agg.TotalDurationMs != 600 {
		t.Errorf("TotalDurationMs = %d, want 600", agg.TotalDurationMs)
	}
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
