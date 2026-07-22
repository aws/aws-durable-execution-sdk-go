package insight

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestLambdaLogExporter_WritesOneJSONLine(t *testing.T) {
	var buf bytes.Buffer
	exp := &LambdaLogExporter{Writer: &buf}

	rec := WorkflowInsightRecord{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionARN:  "arn:aws:lambda:us-east-1:123456789012:function:f:1",
		Status:        ExecutionStatusSucceeded,
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected output to end with a newline (one line per record), got %q", out)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 line, got %d: %v", len(lines), lines)
	}
	var decoded WorkflowInsightRecord
	if err := json.Unmarshal([]byte(lines[0]), &decoded); err != nil {
		t.Fatalf("expected the single line to be valid JSON: %v", err)
	}
	if decoded.ExecutionARN != rec.ExecutionARN {
		t.Errorf("expected round-tripped ExecutionARN %q, got %q", rec.ExecutionARN, decoded.ExecutionARN)
	}
}

func TestLambdaLogExporter_MultipleExportsAreSeparateLines(t *testing.T) {
	var buf bytes.Buffer
	exp := &LambdaLogExporter{Writer: &buf}

	for i := 0; i < 3; i++ {
		rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:test"}
		if err := exp.Export(context.Background(), rec); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 separate lines, got %d", len(lines))
	}
}

func TestNewLambdaLogExporter_DefaultsToStdout(t *testing.T) {
	exp := NewLambdaLogExporter()
	if exp.Writer != nil {
		t.Errorf("expected NewLambdaLogExporter's Writer to be nil (defaults to os.Stdout at Export time), got %v", exp.Writer)
	}
}
