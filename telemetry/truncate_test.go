package telemetry

import (
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Record.Input and Record.Output are json.RawMessage with omitempty, so an
// empty non-nil value is never rendered. Step 3 of Truncate must treat it
// as absent: dropping it frees no bytes, and the Dropped* flags would then
// claim content that was never in the output.
func TestTruncate_InputOutput_EmptyRawMessageIsAbsent(t *testing.T) {
	tests := []struct {
		name        string
		value       json.RawMessage
		wantDropped bool
	}{
		{"nil", nil, false},
		{"empty non-nil", json.RawMessage{}, false},
		{"value", json.RawMessage(`"x"`), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No operations, so Steps 1 and 2 have nothing to drop and the
			// oversized ARN forces Step 3 for both Input and Output.
			record := Record{
				RecordType:   "DurableExecutionTelemetry",
				ExecutionArn: "arn:" + strings.Repeat("a", 256),
				Input:        tt.value,
				Output:       tt.value,
			}
			out := Truncate(record, 64, nil)
			if out.DroppedInput != tt.wantDropped || out.DroppedOutput != tt.wantDropped {
				t.Errorf("DroppedInput=%v DroppedOutput=%v, want both %v",
					out.DroppedInput, out.DroppedOutput, tt.wantDropped)
			}
			if out.Truncated != tt.wantDropped {
				t.Errorf("Truncated=%v, want %v", out.Truncated, tt.wantDropped)
			}
			if out.DroppedOperations != 0 {
				t.Errorf("DroppedOperations=%d, want 0", out.DroppedOperations)
			}
		})
	}
}

// In the operations array, a cleared Result adds `"truncated":true`. So
// clearing a Result shorter than 7 bytes makes the record larger. The
// record has one long Result and nine Results of `1`. Clearing the long
// Result fits exactly. Truncation must stop there.
func TestTruncate_ClearsOnlyTheResultsNeeded(t *testing.T) {
	ops := []OperationRecord{{ID: "0", Type: "STEP", Status: "SUCCEEDED", Result: json.RawMessage(`"` + strings.Repeat("r", 100) + `"`)}}
	for i := 1; i < 10; i++ {
		ops = append(ops, OperationRecord{ID: strconv.Itoa(i), Type: "STEP", Status: "SUCCEEDED", Result: json.RawMessage(`1`)})
	}
	record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
	want := record
	want.Operations = slices.Clone(ops)
	want.Operations[0].Result = nil
	want.Operations[0].Truncated = true
	want.Truncated = true
	exp := &SQSExporter{}
	b, err := exp.Render(want)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	exp.MaxSizeBytes = len(b)

	out := truncateForExporter(record, exp, exp.Render)

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("DroppedOperations=%d; want only the first Result cleared", out.DroppedOperations)
	}
}

// The by-name rendering omits operations without a name. So dropping one
// of them frees no bytes, and DroppedOperations gains a digit at 10. The
// record fits after dropping 1 operation. Truncation must drop 1, not
// every operation.
func TestTruncate_DropsOnlyTheOperationsNeeded(t *testing.T) {
	ops := []OperationRecord{{ID: "0", Name: "big", Type: "STEP", Status: "FAILED", Error: &ErrorRecord{Type: "E", Message: strings.Repeat("e", 500)}}}
	for i := 1; i < 21; i++ {
		ops = append(ops, OperationRecord{ID: strconv.Itoa(i), Type: "STEP", Status: "SUCCEEDED"})
	}
	record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
	exp := &LambdaLogExporter{}
	b, err := exp.Render(dropOldestForTest(record, 1))
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	exp.MaxSizeBytes = len(b)

	out := truncateForExporter(record, exp, exp.Render)

	if out.DroppedOperations != 1 {
		t.Fatalf("DroppedOperations = %d, want 1", out.DroppedOperations)
	}
}
