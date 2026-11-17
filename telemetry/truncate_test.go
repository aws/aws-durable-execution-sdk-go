package telemetry

import (
	"encoding/json"
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
