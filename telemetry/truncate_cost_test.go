package telemetry

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// truncateForExporter applies the truncation that dispatch applies to
// exp. It sizes with render instead of exp.Render, so a test can count the
// renders.
func truncateForExporter(record Record, exp RenderingSizeLimiter, render Renderer) Record {
	return Truncate(record, exp.MaxRecordSizeBytes(), render)
}

// buildSimpleNamedRecord returns n succeeded steps with unique names,
// timestamps and durations, and no result or error.
func buildSimpleNamedRecord(n int) Record {
	start := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	ops := make([]OperationRecord, n)
	for i := range ops {
		at := start.Add(time.Duration(i) * 10 * time.Millisecond)
		end := at.Add(3 * time.Millisecond)
		duration := int64(3)
		ops[i] = OperationRecord{
			ID: "op-" + strconv.Itoa(i), Name: "step-" + strconv.Itoa(i),
			Type: "STEP", SubType: "Step", Status: "SUCCEEDED",
			StartTimestamp: &at, EndTimestamp: &end, DurationMs: &duration, Attempt: 1,
		}
	}
	return Record{
		RecordType:   "DurableExecutionTelemetry",
		ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:demo:$LATEST/durable-execution/run-1/inv-1",
		Operations:   ops,
	}
}

// buildTwiceNamedRecord returns n operations in which every name occurs
// twice: operation i and operation i+n/2. The newer operation has an
// Error. In operationsByName a name that occurs twice shows no Error. So
// dropping the older operation of a pair makes the newer one's Error
// appear, and the rendered record grows.
func buildTwiceNamedRecord(n int) Record {
	half := n / 2
	ops := make([]OperationRecord, n)
	for i := range ops {
		ops[i] = OperationRecord{
			ID:     "op-" + strconv.Itoa(i),
			Name:   "step-" + strings.Repeat("n", 260) + strconv.Itoa(i%half),
			Type:   "STEP",
			Status: "SUCCEEDED",
		}
		if i >= half {
			ops[i].Status = "FAILED"
			ops[i].Error = &ErrorRecord{Type: "TestError", Message: strings.Repeat("e", 200)}
		}
	}
	return Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
}

// A built-in exporter must size a large record a bounded number of times.
// Rendering once per dropped operation is quadratic in the record size.
func TestTruncate_BuiltInExporterRenderCount(t *testing.T) {
	tests := []struct {
		name   string
		exp    RenderingSizeLimiter
		record Record
	}{
		{"simple named steps, by-name", &CloudWatchLogsExporter{MaxSizeBytes: 256_000}, buildSimpleNamedRecord(2000)},
		{"every name twice, by-name", &LambdaLogExporter{}, buildTwiceNamedRecord(2000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			renders := 0
			counting := func(r Record) ([]byte, error) {
				renders++
				return tt.exp.Render(r)
			}
			out := truncateForExporter(tt.record, tt.exp, counting)
			if out.DroppedOperations == 0 {
				t.Fatalf("DroppedOperations = 0; the record must be over the limit")
			}
			if renders >= 150 {
				t.Fatalf("truncation rendered the record %d times, want fewer than 150", renders)
			}
		})
	}
}
