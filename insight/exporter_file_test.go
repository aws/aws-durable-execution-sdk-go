package insight

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileExporter_NDJSONMode_AppendsToDailyFile(t *testing.T) {
	dir := t.TempDir()
	exp := &FileExporter{Directory: dir, Mode: FileModeNDJSON}

	for i := 0; i < 3; i++ {
		rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:test"}
		if err := exp.Export(context.Background(), rec); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}

	today := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(dir, today+".ndjson")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected file %s to exist: %v", path, err)
	}

	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines (one per Export call), got %d", len(lines))
	}
	for _, line := range lines {
		var decoded WorkflowInsightRecord
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("expected each line to be valid JSON: %v", err)
		}
	}
}

func TestFileExporter_JSONMode_OneFilePerExecution_Overwrites(t *testing.T) {
	dir := t.TempDir()
	exp := &FileExporter{Directory: dir, Mode: FileModeJSON}

	rec1 := WorkflowInsightRecord{ExecutionName: "exec-1", Status: ExecutionStatusRunning}
	if err := exp.Export(context.Background(), rec1); err != nil {
		t.Fatalf("first Export: %v", err)
	}
	rec2 := WorkflowInsightRecord{ExecutionName: "exec-1", Status: ExecutionStatusSucceeded}
	if err := exp.Export(context.Background(), rec2); err != nil {
		t.Fatalf("second Export: %v", err)
	}

	path := filepath.Join(dir, "exec-1.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected file %s to exist: %v", path, err)
	}
	var decoded WorkflowInsightRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
	if decoded.Status != ExecutionStatusSucceeded {
		t.Errorf("expected the SECOND Export to have overwritten the file (Status=SUCCEEDED), got %s", decoded.Status)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 file (overwritten, not duplicated), got %d: %v", len(entries), entries)
	}
}

func TestFileExporter_JSONMode_FallsBackToSanitizedARN(t *testing.T) {
	dir := t.TempDir()
	exp := &FileExporter{Directory: dir, Mode: FileModeJSON}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 file, got %d", len(entries))
	}
	if strings.Contains(entries[0].Name(), ":") || strings.Contains(entries[0].Name(), "/") {
		t.Errorf("expected the ARN-derived filename to have ':' and '/' sanitized out, got %q", entries[0].Name())
	}
}

func TestFileExporter_DefaultMode_IsNDJSON(t *testing.T) {
	dir := t.TempDir()
	exp := &FileExporter{Directory: dir} // Mode left unset

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	today := time.Now().UTC().Format("2006-01-02")
	if _, err := os.Stat(filepath.Join(dir, today+".ndjson")); err != nil {
		t.Errorf("expected the default (unset) Mode to behave like FileModeNDJSON, got: %v", err)
	}
}

func TestFileExporter_OperationsFormatByName(t *testing.T) {
	dir := t.TempDir()
	exp := &FileExporter{Directory: dir, Mode: FileModeJSON, OperationsFormat: OperationsFormatByName}

	rec := WorkflowInsightRecord{ExecutionName: "exec-1", Operations: []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}}}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "exec-1.json"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' to be absent under OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' to be present under OperationsFormatByName")
	}
}

func TestFileExporter_MissingDirectory_ReturnsError(t *testing.T) {
	exp := &FileExporter{}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when Directory is unset")
	}
}

func TestFileExporter_NonexistentDirectory_ReturnsError(t *testing.T) {
	exp := &FileExporter{Directory: "/nonexistent/path/that/should/not/exist"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err == nil {
		t.Fatal("expected an error when Directory does not exist")
	}
}
