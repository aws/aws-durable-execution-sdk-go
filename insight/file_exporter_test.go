package insight

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileExporter_WritesJSONFile(t *testing.T) {
	dir := t.TempDir()
	exp := NewFileExporter(FileExporterConfig{Dir: dir})

	rec := Record{
		RecordType:    RecordType,
		SchemaVersion: SchemaVersion,
		ExecutionArn:  "arn:aws:lambda:us-east-1:123:function:f:1",
		Operations:    []OperationRecord{},
	}

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}

	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var decoded Record
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
	if decoded.ExecutionArn != rec.ExecutionArn {
		t.Errorf("expected ExecutionArn %q, got %q", rec.ExecutionArn, decoded.ExecutionArn)
	}
}

func TestFileExporter_DefaultFileNamer_Pattern(t *testing.T) {
	dir := t.TempDir()
	exp := NewFileExporter(FileExporterConfig{Dir: dir})

	rec := Record{
		ExecutionArn: "arn:aws:lambda:us-east-1:123:function:f:1",
		Operations:   []OperationRecord{},
	}

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}

	name := entries[0].Name()
	if !strings.HasSuffix(name, ".json") {
		t.Errorf("expected .json suffix, got %q", name)
	}
	if !strings.Contains(name, "_") {
		t.Errorf("expected underscore separator in filename, got %q", name)
	}
}

func TestFileExporter_CustomFileNamer(t *testing.T) {
	dir := t.TempDir()
	exp := NewFileExporter(FileExporterConfig{
		Dir: dir,
		FileNamer: func(record *Record) string {
			return record.ExecutionName + ".json"
		},
	})

	rec := Record{
		ExecutionArn:  "arn:test",
		ExecutionName: "my-execution",
		Operations:    []OperationRecord{},
	}

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	path := filepath.Join(dir, "my-execution.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file at %s: %v", path, err)
	}
}

func TestFileExporter_CreatesDirIfMissing(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "a", "b", "c")

	exp := NewFileExporter(FileExporterConfig{Dir: nested})

	rec := Record{ExecutionArn: "arn:test", Operations: []OperationRecord{}}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	entries, err := os.ReadDir(nested)
	if err != nil {
		t.Fatalf("expected nested dir to exist: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}
}

func TestFileExporter_MultipleRecords_SeparateFiles(t *testing.T) {
	dir := t.TempDir()
	exp := NewFileExporter(FileExporterConfig{Dir: dir})

	for i := 0; i < 3; i++ {
		rec := Record{
			ExecutionArn: "arn:test:" + strings.Repeat("x", i+1),
			Operations:   []OperationRecord{},
		}
		if err := exp.Export(context.Background(), rec); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 files (one per record), got %d", len(entries))
	}
}

func TestFileExporter_OperationsFormat(t *testing.T) {
	dir := t.TempDir()
	exp := NewFileExporter(FileExporterConfig{
		Dir:              dir,
		OperationsFormat: OperationsFormatByName,
		FileNamer:        func(r *Record) string { return "out.json" },
	})

	rec := Record{
		ExecutionArn: "arn:test",
		Operations:   []OperationRecord{{ID: "1", Name: "step1", Type: "STEP", Status: "SUCCEEDED"}},
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "out.json"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' absent under OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' present under OperationsFormatByName")
	}
}

func TestFileExporter_PanicsOnEmptyDir(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty Dir")
		}
	}()
	NewFileExporter(FileExporterConfig{})
}

func TestFileExporter_Close(t *testing.T) {
	exp := NewFileExporter(FileExporterConfig{Dir: t.TempDir()})
	if err := exp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
