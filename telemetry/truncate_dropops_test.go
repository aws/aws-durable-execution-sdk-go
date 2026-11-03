package telemetry

import (
	"encoding/json"
	"strings"
	"testing"
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

	renders := 0
	counting := func(r Record) ([]byte, error) {
		renders++
		return json.Marshal(r)
	}

	out := Truncate(record, maxBytes, counting)

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

	// The whole point of the fix: a handful of renders, not thousands.
	// Step 1 (per-op Result drop) + binary search is well under this bound;
	// the old one-op-at-a-time loop would render many thousands of times.
	if renders > 100 {
		t.Fatalf("Truncate rendered %d times for %d operations; expected O(log n) (<=100)", renders, n)
	}
}
