package durable

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileSystemSerdesAlwaysMode(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)

	type Item struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}
	input := Item{Name: "test", Value: 42}

	data, err := s.Marshal(context.Background(), SerdesContext{}, input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Verify envelope references a file.
	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.File == "" {
		t.Fatal("expected file pointer in envelope")
	}
	if env.Data != nil {
		t.Fatal("expected no inline data in ALWAYS mode")
	}
	if !strings.HasPrefix(env.File, dir) {
		t.Errorf("file %q not under basePath %q", env.File, dir)
	}

	// Verify file exists and contains valid JSON.
	content, err := os.ReadFile(env.File)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	var fromFile Item
	if err := json.Unmarshal(content, &fromFile); err != nil {
		t.Fatalf("unmarshal from file: %v", err)
	}
	if fromFile != input {
		t.Errorf("file content = %+v, want %+v", fromFile, input)
	}

	// Unmarshal from envelope.
	var output Item
	if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &output); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if output != input {
		t.Errorf("Unmarshal result = %+v, want %+v", output, input)
	}
}

func TestFileSystemSerdesOverflowSmall(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir, FileSystemSerdesConfig{
		Mode: FileSystemSerdesModeOverflow,
	})

	// Small value stays inline.
	input := "small"
	data, err := s.Marshal(context.Background(), SerdesContext{}, input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Data == nil {
		t.Fatal("expected inline data for small value")
	}
	if env.File != "" {
		t.Fatal("expected no file for small value")
	}

	// Unmarshal.
	var output string
	if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &output); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if output != input {
		t.Errorf("output = %q, want %q", output, input)
	}
}

func TestFileSystemSerdesOverflowLarge(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir, FileSystemSerdesConfig{
		Mode: FileSystemSerdesModeOverflow,
	})

	// Large value overflows to file.
	input := strings.Repeat("x", 300*1024) // 300KB, exceeds 255KB threshold
	data, err := s.Marshal(context.Background(), SerdesContext{}, input)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.File == "" {
		t.Fatal("expected file pointer for large value")
	}
	if env.Data != nil {
		t.Fatal("expected no inline data for large value")
	}

	// Unmarshal from file.
	var output string
	if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &output); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if output != input {
		t.Errorf("output length = %d, want %d", len(output), len(input))
	}
}

func TestFileSystemSerdesNil(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)

	data, err := s.Marshal(context.Background(), SerdesContext{}, nil)
	if err != nil {
		t.Fatalf("Marshal nil: %v", err)
	}
	if string(data) != "null" {
		t.Errorf("Marshal nil = %s, want null", data)
	}

	var output *string
	if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &output); err != nil {
		t.Fatalf("Unmarshal null: %v", err)
	}
	if output != nil {
		t.Errorf("output = %v, want nil", output)
	}
}

func TestFileSystemSerdesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name  string
		mode  FileSystemSerdesMode
		input any
	}{
		{"always/struct", FileSystemSerdesModeAlways, map[string]any{"key": "value"}},
		{"always/int", FileSystemSerdesModeAlways, 123},
		{"always/slice", FileSystemSerdesModeAlways, []int{1, 2, 3}},
		{"overflow/small", FileSystemSerdesModeOverflow, "hello"},
		{"overflow/struct", FileSystemSerdesModeOverflow, map[string]string{"a": "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewFileSystemSerdes(dir, FileSystemSerdesConfig{Mode: tt.mode})
			data, err := s.Marshal(context.Background(), SerdesContext{}, tt.input)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			// Unmarshal into any for comparison.
			var output any
			if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &output); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			// Compare via JSON re-encode.
			wantJSON, _ := json.Marshal(tt.input)
			gotJSON, _ := json.Marshal(output)
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("got %s, want %s", gotJSON, wantJSON)
			}
		})
	}
}

func TestFileSystemSerdesHashPathEncoding(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir, FileSystemSerdesConfig{})

	data, err := s.Marshal(context.Background(), SerdesContext{}, "test-value")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	// File path should use hex-encoded segments.
	rel, _ := filepath.Rel(dir, env.File)
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 2 {
		t.Fatalf("expected 2 path segments, got %d: %v", len(parts), parts)
	}
	// Both segments should be hex strings (32 hex chars each from 16 bytes).
	for _, p := range parts {
		p = strings.TrimSuffix(p, ".json")
		if len(p) != 32 {
			t.Errorf("path segment %q has length %d, want 32", p, len(p))
		}
	}
}

func TestFileSystemSerdesEmptyPayload(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)

	// Empty string "" is a valid JSON value.
	data, err := s.Marshal(context.Background(), SerdesContext{}, "")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var output string
	if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &output); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if output != "" {
		t.Errorf("output = %q, want empty", output)
	}
}

func TestFileSystemSerdesUnmarshalEmptyEnvelope(t *testing.T) {
	// Unmarshal with empty data should handle gracefully.
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)

	var output any
	err := s.Unmarshal(context.Background(), SerdesContext{}, []byte("null"), &output)
	if err != nil {
		t.Fatalf("Unmarshal null: %v", err)
	}
	if output != nil {
		t.Errorf("output = %v, want nil", output)
	}
}

func TestFileSystemSerdesMissingFile(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)

	// Craft an envelope pointing to a non-existent file.
	env := fsEnvelope{File: filepath.Join(dir, "nonexistent.json")}
	data, _ := json.Marshal(env)

	var output any
	err := s.Unmarshal(context.Background(), SerdesContext{}, data, &output)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "read file") {
		t.Errorf("error = %v, want read file error", err)
	}
}

// tempFilesUnder returns every file under root whose name ends with the
// temporary-write suffix.
func tempFilesUnder(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), fileSystemSerdesTempSuffix) {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return found
}

func TestFileSystemSerdesWriteLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)
	meta := SerdesContext{DurableExecutionArn: "arn:test", OperationID: "op-1"}

	data, err := s.Marshal(context.Background(), meta, map[string]int{"a": 1})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if strings.HasSuffix(env.File, fileSystemSerdesTempSuffix) {
		t.Errorf("envelope references temporary file %q", env.File)
	}
	info, err := os.Stat(env.File)
	if err != nil {
		t.Fatalf("target file missing: %v", err)
	}
	// A new file gets the same permissions a direct create would, so the
	// process umask applies. Compare against a reference file created the
	// direct way rather than a fixed value.
	ref := filepath.Join(dir, "reference.json")
	if err := os.WriteFile(ref, []byte("{}"), fileSystemSerdesNewFileMode); err != nil {
		t.Fatalf("write reference: %v", err)
	}
	refInfo, err := os.Stat(ref)
	if err != nil {
		t.Fatalf("stat reference: %v", err)
	}
	if got, want := info.Mode().Perm(), refInfo.Mode().Perm(); got != want {
		t.Errorf("new file mode = %o, want %o (same as a direct create)", got, want)
	}
	if left := tempFilesUnder(t, dir); len(left) != 0 {
		t.Errorf("temporary files left after successful write: %v", left)
	}
}

func TestFileSystemSerdesOverwritePreservesRestrictiveMode(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)
	meta := SerdesContext{DurableExecutionArn: "arn:test", OperationID: "op-1"}

	data, err := s.Marshal(context.Background(), meta, "first")
	if err != nil {
		t.Fatalf("Marshal first: %v", err)
	}
	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	// An operator tightens the payload's permissions. A later overwrite of
	// the same operation must not widen them.
	if err := os.Chmod(env.File, 0o600); err != nil {
		t.Fatalf("chmod target: %v", err)
	}

	data, err = s.Marshal(context.Background(), meta, "second")
	if err != nil {
		t.Fatalf("Marshal second: %v", err)
	}
	info, err := os.Stat(env.File)
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("mode after overwrite = %o, want 600", got)
	}
	var out string
	if err := s.Unmarshal(context.Background(), meta, data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != "second" {
		t.Errorf("Unmarshal result = %q, want %q", out, "second")
	}
	if left := tempFilesUnder(t, dir); len(left) != 0 {
		t.Errorf("temporary files left after overwrite: %v", left)
	}
}

func TestFileSystemSerdesFailedWriteRemovesTempFile(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)
	meta := SerdesContext{DurableExecutionArn: "arn:test", OperationID: "op-1"}

	// Write a first version so the failure case can show the previous
	// complete file is preserved.
	data, err := s.Marshal(context.Background(), meta, "first")
	if err != nil {
		t.Fatalf("Marshal first: %v", err)
	}
	var env fsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}

	// Inject a sync failure for the second write.
	orig := syncFile
	t.Cleanup(func() { syncFile = orig })
	syncFile = func(*os.File) error { return errors.New("injected sync failure") }

	if _, err := s.Marshal(context.Background(), meta, "second"); err == nil {
		t.Fatal("expected Marshal to fail when sync fails")
	} else if !strings.Contains(err.Error(), "injected sync failure") {
		t.Errorf("error = %v, want injected sync failure", err)
	}

	if left := tempFilesUnder(t, dir); len(left) != 0 {
		t.Errorf("temporary files left after failed write: %v", left)
	}
	content, err := os.ReadFile(env.File)
	if err != nil {
		t.Fatalf("read previous file: %v", err)
	}
	if string(content) != `"first"` {
		t.Errorf("previous file content = %s, want %q", content, `"first"`)
	}
}

func TestFileSystemSerdesOverwriteReplacesWholeFile(t *testing.T) {
	dir := t.TempDir()
	s := NewFileSystemSerdes(dir)
	meta := SerdesContext{DurableExecutionArn: "arn:test", OperationID: "op-1"}

	if _, err := s.Marshal(context.Background(), meta, strings.Repeat("x", 1000)); err != nil {
		t.Fatalf("Marshal first: %v", err)
	}
	data, err := s.Marshal(context.Background(), meta, "short")
	if err != nil {
		t.Fatalf("Marshal second: %v", err)
	}

	// A shorter second write must fully replace the longer first one, not
	// leave trailing bytes from it.
	var out string
	if err := s.Unmarshal(context.Background(), meta, data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != "short" {
		t.Errorf("Unmarshal result = %q, want %q", out, "short")
	}
	if left := tempFilesUnder(t, dir); len(left) != 0 {
		t.Errorf("temporary files left after overwrite: %v", left)
	}
}
