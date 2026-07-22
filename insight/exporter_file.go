package insight

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FileMode selects how FileExporter lays out records on disk, matching
// the JS SDK's own FileExporter mode option.
type FileMode string

const (
	// FileModeNDJSON appends each record as one line to a daily file
	// ("{YYYY-MM-DD}.ndjson") - good for bulk, append-only processing.
	// The default.
	FileModeNDJSON FileMode = "ndjson"

	// FileModeJSON writes one file per execution
	// ("{ExecutionName}.json"), OVERWRITING that file on every Export
	// for the same execution (upsert-by-filename), matching the JS SDK's
	// own documented behavior.
	FileModeJSON FileMode = "json"
)

// FileExporter writes records to the filesystem - an EFS mount, an S3
// File Gateway-backed NFS share, or (for local development/testing
// only) a plain local directory - matching the JS SDK's own
// FileExporter. Uses only the standard library (os/io), no AWS SDK
// dependency, matching the JS SDK's own "no AWS SDK dependencies - uses
// node:fs/promises" note.
//
// # Concurrency
//
// Matching real POSIX append semantics: FileModeNDJSON opens its target
// file with os.O_APPEND on every single Export call (rather than
// keeping a single os.File open across calls) specifically so that
// O_APPEND's own kernel-level guarantee - each individual write() is
// atomically positioned at the current end-of-file - protects against
// interleaved writes from concurrent Lambda invocations (or concurrent
// goroutines within one invocation, e.g. multiple exporters, though this
// package's own Plugin.dispatch never calls the SAME exporter
// concurrently for a single execution) sharing the same EFS-mounted
// directory. This guarantee holds for local POSIX filesystems and for
// EFS specifically (EFS documents append-mode writes as atomic up to its
// own I/O size limits) - it does NOT necessarily hold for every possible
// NFS-backed mount (e.g. some S3 File Gateway configurations), which is
// a real, inherent limitation of this exporter's own append-based
// design for FileModeNDJSON specifically, not something this
// implementation can paper over.
type FileExporter struct {
	// Directory is the target directory. Required. Must already exist -
	// this exporter never creates it (matching the JS SDK's own
	// documented setup: "Attach an EFS file system... configure the
	// file system," i.e. directory provisioning is the caller's
	// infrastructure concern, not something a plugin should do at
	// runtime).
	Directory string

	// Mode selects the on-disk layout - see FileMode's own doc. Defaults
	// to FileModeNDJSON when left at its zero value.
	Mode FileMode

	// OperationsFormat controls how the record's own operations are
	// rendered - see that type's own doc. Defaults to
	// OperationsFormatArray when left at its zero value.
	OperationsFormat OperationsFormat

	// MaxSizeBytes, if positive, enables size-based truncation (see
	// SizeLimiter's own doc) at that limit. Left at its zero value (or
	// negative), truncation is DISABLED for this exporter - matching the
	// JS SDK's own documented lack of a default limit for FileExporter
	// specifically (unlike every AWS-service exporter in this package,
	// which has a real, positive default instead).
	MaxSizeBytes int
}

// Export writes record to this exporter's target directory, per Mode.
func (e *FileExporter) Export(_ context.Context, record WorkflowInsightRecord) error {
	if e.Directory == "" {
		return fmt.Errorf("insight.FileExporter: Directory is required")
	}

	body, err := renderRecord(record, e.OperationsFormat)
	if err != nil {
		return fmt.Errorf("insight.FileExporter: rendering record: %w", err)
	}

	if e.Mode == FileModeJSON {
		return e.writeJSONFile(record, body)
	}
	return e.appendNDJSON(body)
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes -
// which, unlike every AWS-service exporter in this package, has NO
// positive default: 0 (or a negative value) genuinely means "truncation
// disabled" here, matching FileExporter's own documented lack of a
// default limit.
func (e *FileExporter) MaxRecordSizeBytes() int {
	return e.MaxSizeBytes
}

// Render implements RenderingSizeLimiter, returning the EXACT bytes
// Export would write, respecting BOTH e's own OperationsFormat AND Mode
// (FileModeNDJSON appends a trailing newline that FileModeJSON does
// not - see appendNDJSON/writeJSONFile), so Truncate's own
// fits-or-doesn't-fit sizing reflects the real on-disk form.
func (e *FileExporter) Render(record WorkflowInsightRecord) ([]byte, error) {
	b, err := renderRecord(record, e.OperationsFormat)
	if err != nil {
		return nil, err
	}
	if e.Mode == FileModeJSON {
		return b, nil
	}
	return append(b, '\n'), nil
}

// writeJSONFile implements FileModeJSON: one file per execution,
// overwritten on every Export for the same execution.
func (e *FileExporter) writeJSONFile(record WorkflowInsightRecord, body []byte) error {
	name := record.ExecutionName
	if name == "" {
		name = sanitizeForKey(record.ExecutionARN)
	}
	if name == "" {
		name = "unknown"
	}
	path := filepath.Join(e.Directory, name+".json")

	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("insight.FileExporter: writing %s: %w", path, err)
	}
	return nil
}

// appendNDJSON implements FileModeNDJSON: append one line to today's
// (UTC) daily file - see this type's own doc for the O_APPEND
// atomicity reasoning.
func (e *FileExporter) appendNDJSON(body []byte) error {
	today := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(e.Directory, today+".ndjson")

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("insight.FileExporter: opening %s: %w", path, err)
	}
	defer f.Close()

	line := append(append([]byte{}, body...), '\n')
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("insight.FileExporter: writing %s: %w", path, err)
	}
	return nil
}
