package telemetry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// buildOversizedRecord returns a record with n operations whose rendered size
// comfortably exceeds maxBytes, so Truncate must drop whole operations.
func buildOversizedRecord(n int) Record {
	ops := make([]OperationRecord, n)
	for i := range ops {
		ops[i] = OperationRecord{
			ID:     "op-" + strings.Repeat("x", 8),
			Type:   "step",
			Status: "SUCCEEDED",
			Result: &ContentField{Value: strings.Repeat("y", 64)},
		}
	}
	return Record{
		RecordType:   "DurableExecutionTelemetry",
		ExecutionArn: "arn:test",
		Operations:   ops,
	}
}

// Truncate must not re-render the whole record once per dropped operation.
// With tens of thousands of operations that O(n) render count stalls for
// minutes; the binary search keeps it to O(log n).
func TestTruncate_DropOperations_RenderCountIsLogarithmic(t *testing.T) {
	const n = 20000
	record := buildOversizedRecord(n)

	full, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// A cap that forces dropping the vast majority of operations.
	maxBytes := len(full) / 50

	start := time.Now()
	out := Truncate(record, maxBytes, nil)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("nil-renderer Truncate took %s for %d operations; expected O(log n)", elapsed, n)
	}

	// Must actually fit now, with the final metadata accounted for.
	if b, _ := json.Marshal(out); len(b) > maxBytes {
		t.Fatalf("result is %d bytes, exceeds cap %d", len(b), maxBytes)
	}
	if !out.Truncated || out.DroppedOperations == 0 {
		t.Fatalf("expected Truncated with DroppedOperations>0, got Truncated=%v Dropped=%d",
			out.Truncated, out.DroppedOperations)
	}
	// Retention is "oldest dropped": kept ops are the newest suffix.
	if got := out.DroppedOperations + len(out.Operations); got != n {
		t.Fatalf("dropped(%d)+kept(%d) = %d, want %d", out.DroppedOperations, len(out.Operations), got, n)
	}

	// The nil canonical renderer keeps the O(log n) binary-search path.
	// A supplied renderer cannot be assumed monotonic, so it walks
	// oldest-first instead; this test must use nil to stay logarithmic.
}
