package insight

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// FileExporterConfig configures a FileExporter.
type FileExporterConfig struct {
	// Dir is the directory where record files are written. Required.
	// Created via os.MkdirAll if it does not exist.
	Dir string

	// FileNamer returns the filename (without directory) for a given
	// record. If nil, DefaultFileNamer is used.
	FileNamer func(record *Record) string

	// OperationsFormat controls how operations are rendered in the JSON
	// output. Defaults to OperationsFormatArray when empty.
	OperationsFormat OperationsFormat
}

// FileExporter writes each record as a JSON file to a configurable
// directory on the local filesystem. It implements Exporter and requires
// no AWS dependencies — only the standard library.
type FileExporter struct {
	dir              string
	fileNamer        func(record *Record) string
	operationsFormat OperationsFormat
}

// NewFileExporter returns a FileExporter configured by cfg. Panics if
// cfg.Dir is empty.
func NewFileExporter(cfg FileExporterConfig) *FileExporter {
	if cfg.Dir == "" {
		panic("insight.NewFileExporter: Dir is required")
	}
	namer := cfg.FileNamer
	if namer == nil {
		namer = DefaultFileNamer
	}
	return &FileExporter{
		dir:              cfg.Dir,
		fileNamer:        namer,
		operationsFormat: cfg.OperationsFormat,
	}
}

// DefaultFileNamer returns a filename based on the SHA-256 hash of the
// execution ARN and the current timestamp in UnixNano:
// "{hash8}_{timestamp}.json".
func DefaultFileNamer(record *Record) string {
	h := sha256.Sum256([]byte(record.ExecutionArn))
	hash8 := hex.EncodeToString(h[:4])
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	return hash8 + "_" + ts + ".json"
}

// Export writes record as a JSON file in the configured directory. The
// directory is created if it does not exist.
func (e *FileExporter) Export(_ context.Context, record Record) error {
	if err := os.MkdirAll(e.dir, 0o755); err != nil {
		return fmt.Errorf("insight.FileExporter: creating directory %s: %w", e.dir, err)
	}

	body, err := RenderRecord(record, e.operationsFormat)
	if err != nil {
		return fmt.Errorf("insight.FileExporter: rendering record: %w", err)
	}

	name := e.fileNamer(&record)
	path := filepath.Join(e.dir, name)

	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("insight.FileExporter: writing %s: %w", path, err)
	}
	return nil
}

// Close is a no-op — FileExporter holds no persistent connections.
func (e *FileExporter) Close() error {
	return nil
}
