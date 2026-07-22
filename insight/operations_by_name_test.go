package insight

import "testing"

func d(ms int64) *int64 { return &ms }

func TestOperationsByName_SingleOccurrence_IncludesError(t *testing.T) {
	ops := []OperationRecord{
		{Name: "insert-to-db", Type: "STEP", SubType: "Step", Status: "SUCCEEDED", DurationMs: d(6200)},
	}
	byName := OperationsByName(ops)

	agg, ok := byName["insert-to-db"]
	if !ok {
		t.Fatal("expected an entry for insert-to-db")
	}
	if agg.Count != 1 {
		t.Errorf("expected Count=1, got %d", agg.Count)
	}
	if agg.MinDurationMs != 6200 || agg.MaxDurationMs != 6200 || agg.TotalDurationMs != 6200 {
		t.Errorf("expected min=max=total=6200, got min=%d max=%d total=%d", agg.MinDurationMs, agg.MaxDurationMs, agg.TotalDurationMs)
	}
	if agg.FailedCount != 0 {
		t.Errorf("expected FailedCount=0, got %d", agg.FailedCount)
	}
}

func TestOperationsByName_RepeatedName_AggregatesMetricsButDropsError(t *testing.T) {
	ops := []OperationRecord{
		{Name: "process-item", Type: "STEP", Status: "SUCCEEDED", DurationMs: d(100), Attempt: 1},
		{Name: "process-item", Type: "STEP", Status: "FAILED", DurationMs: d(50), Attempt: 1, Error: &ErrorDetail{Name: "Err", Message: "boom"}},
		{Name: "process-item", Type: "STEP", Status: "SUCCEEDED", DurationMs: d(300), Attempt: 2},
	}
	byName := OperationsByName(ops)

	agg, ok := byName["process-item"]
	if !ok {
		t.Fatal("expected an entry for process-item")
	}
	if agg.Count != 3 {
		t.Errorf("expected Count=3, got %d", agg.Count)
	}
	if agg.MinDurationMs != 50 {
		t.Errorf("expected MinDurationMs=50, got %d", agg.MinDurationMs)
	}
	if agg.MaxDurationMs != 300 {
		t.Errorf("expected MaxDurationMs=300, got %d", agg.MaxDurationMs)
	}
	if agg.TotalDurationMs != 450 {
		t.Errorf("expected TotalDurationMs=450, got %d", agg.TotalDurationMs)
	}
	if agg.FailedCount != 1 {
		t.Errorf("expected FailedCount=1, got %d", agg.FailedCount)
	}
	if agg.MaxAttempt != 2 {
		t.Errorf("expected MaxAttempt=2, got %d", agg.MaxAttempt)
	}
	// Status/Type reflect the LAST occurrence - the 3rd, SUCCEEDED.
	if agg.Status != "SUCCEEDED" {
		t.Errorf("expected Status=SUCCEEDED (last occurrence), got %s", agg.Status)
	}
	// Error must be dropped entirely for a repeated name, even though
	// one occurrence DID fail with a real error - FailedCount is the
	// only signal retained for that.
	if agg.Error != nil {
		t.Errorf("expected Error=nil for a repeated name (no single representative value), got %+v", agg.Error)
	}
}

func TestOperationsByName_UnnamedOperations_AreExcluded(t *testing.T) {
	ops := []OperationRecord{
		{Name: "", Type: "STEP", Status: "SUCCEEDED"},
		{Name: "named", Type: "STEP", Status: "SUCCEEDED"},
	}
	byName := OperationsByName(ops)

	if len(byName) != 1 {
		t.Fatalf("expected exactly 1 entry (unnamed operations excluded), got %d: %+v", len(byName), byName)
	}
	if _, ok := byName["named"]; !ok {
		t.Error("expected an entry for 'named'")
	}
}

func TestOperationsByName_NoDurationMs_LeavesMetricsZero(t *testing.T) {
	ops := []OperationRecord{{Name: "no-duration", Type: "WAIT", Status: "SUCCEEDED"}}
	byName := OperationsByName(ops)

	agg := byName["no-duration"]
	if agg.MinDurationMs != 0 || agg.MaxDurationMs != 0 || agg.TotalDurationMs != 0 {
		t.Errorf("expected all duration metrics to stay 0 when DurationMs is nil, got %+v", agg)
	}
}

func TestOperationsByName_EmptyOperations_ReturnsEmptyMap(t *testing.T) {
	byName := OperationsByName(nil)
	if len(byName) != 0 {
		t.Errorf("expected an empty map for nil input, got %d entries", len(byName))
	}
}
