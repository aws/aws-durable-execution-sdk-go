package telemetry

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// buildOversizedRecord returns a record with n operations whose rendered size
// comfortably exceeds maxBytes, so Truncate must drop whole operations. The
// operation IDs are "op-0" (oldest) to "op-<n-1>" (newest).
func buildOversizedRecord(n int) Record {
	ops := make([]OperationRecord, n)
	for i := range ops {
		ops[i] = OperationRecord{
			ID:     "op-" + strconv.Itoa(i),
			Type:   "step",
			Status: "SUCCEEDED",
			Result: json.RawMessage(`"` + strings.Repeat("y", 64) + `"`),
		}
	}
	return Record{
		RecordType:   "DurableExecutionTelemetry",
		ExecutionArn: "arn:test",
		Operations:   ops,
	}
}

// When clearing every result is not enough, Truncate drops whole
// operations, oldest first. It must keep the newest operations, and it must
// drop only as many as the limit requires.
func TestTruncate_DropOperations_KeepsNewestThatFit(t *testing.T) {
	const n = 500
	record := buildOversizedRecord(n)

	full, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// A cap that forces dropping the vast majority of operations.
	maxBytes := len(full) / 50

	out := Truncate(record, maxBytes, nil)

	// Must actually fit now, with the final metadata accounted for.
	if b, _ := json.Marshal(out); len(b) > maxBytes {
		t.Fatalf("result is %d bytes, exceeds cap %d", len(b), maxBytes)
	}
	dropped := out.DroppedOperations
	if !out.Truncated || dropped == 0 {
		t.Fatalf("expected Truncated with DroppedOperations>0, got Truncated=%v Dropped=%d",
			out.Truncated, dropped)
	}
	if len(out.Operations) == 0 {
		t.Fatalf("every operation was dropped; the cap %d leaves room for some", maxBytes)
	}
	if got := dropped + len(out.Operations); got != n {
		t.Fatalf("dropped(%d)+kept(%d) = %d, want %d", dropped, len(out.Operations), got, n)
	}

	// The kept operations are the newest ones, in order.
	for i, op := range out.Operations {
		if want := "op-" + strconv.Itoa(dropped+i); op.ID != want {
			t.Fatalf("kept operation %d has ID %q, want %q", i, op.ID, want)
		}
	}

	// Dropping one operation fewer would not fit. The extra operation is
	// the newest dropped one, in the state the result-clearing step left it.
	oneFewer := out
	extra := record.Operations[dropped-1]
	extra.Result = nil
	extra.Truncated = true
	oneFewer.Operations = append([]OperationRecord{extra}, out.Operations...)
	oneFewer.DroppedOperations = dropped - 1
	if b, _ := json.Marshal(oneFewer); len(b) <= maxBytes {
		t.Fatalf("dropping %d operations also fits (%d bytes, cap %d); Truncate dropped %d",
			dropped-1, len(b), maxBytes, dropped)
	}
}
