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

// buildResultFreeRecord is the IncludeOperationResults:false shape:
// many operations, none with a Result.
func buildResultFreeRecord(n int) Record {
	ops := make([]OperationRecord, n)
	for i := range ops {
		ops[i] = OperationRecord{
			ID:     "op-" + strings.Repeat("x", 8),
			Type:   "step",
			Status: "SUCCEEDED",
		}
	}
	return Record{
		RecordType:   "DurableExecutionTelemetry",
		ExecutionArn: "arn:test",
		Operations:   ops,
	}
}

// Ordered truncation must not re-render once per operation when every
// Result is nil. SQS, EventBridge, and Firehose all pass a renderer, so
// IncludeOperationResults:false used to copy and encode n times before
// the first whole-operation drop (n+6 = 20006 calls at n=20000).
func TestTruncate_CustomRenderer_ResultFreeRenderCountIsBounded(t *testing.T) {
	const n = 20000
	const bytesPerOp = 64
	record := buildResultFreeRecord(n)

	calls := 0
	buf := make([]byte, bytesPerOp*n+128)
	render := func(r Record) ([]byte, error) {
		calls++
		size := bytesPerOp * len(r.Operations)
		if r.Truncated {
			size += 32
		}
		size += 8 * r.DroppedOperations
		if size > len(buf) {
			return make([]byte, size), nil
		}
		return buf[:size:size], nil
	}

	// Just under the full record. Dropping one 64-byte operation more
	// than pays for truncation metadata, so the first whole-op drop fits.
	maxBytes := bytesPerOp*n - 1
	out := Truncate(record, maxBytes, render)

	if !out.Truncated {
		t.Fatal("expected Truncated=true")
	}
	if out.DroppedOperations != 1 {
		t.Fatalf("DroppedOperations=%d, want 1 (first whole-op drop)", out.DroppedOperations)
	}
	if got := out.DroppedOperations + len(out.Operations); got != n {
		t.Fatalf("dropped(%d)+kept(%d) = %d, want %d", out.DroppedOperations, len(out.Operations), got, n)
	}

	// Initial size check, k=0 and k=1 operation-drop probes, final check.
	// The result phase must not add a call per operation (that was n+6).
	if calls != 4 {
		t.Fatalf("renderer called %d times for %d result-free operations; want 4", calls, n)
	}
}
