package insight

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// LambdaLogExporter writes records as a single JSON line to stdout (via
// fmt.Fprintln by default), matching the JS SDK's own LambdaLogExporter:
// since Lambda captures stdout to the function's own CloudWatch log
// group automatically, this requires ZERO IAM permissions and ZERO
// setup - the default, "getting started" exporter.
//
// The zero value is ready to use (writes to os.Stdout); set Writer to
// redirect output, e.g. in a test.
type LambdaLogExporter struct {
	// Writer overrides the default destination (os.Stdout) - primarily
	// for tests. A nil Writer (the zero value) writes to os.Stdout.
	Writer io.Writer

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesLog (see
	// SizeLimiter's own doc for the truncation contract this enables).
	// Left at its zero value, MaxRecordSizeBytes() returns
	// DefaultMaxRecordSizeBytesLog instead.
	MaxSizeBytes int
}

// NewLambdaLogExporter returns a LambdaLogExporter writing to os.Stdout,
// matching the JS SDK's own `new LambdaLogExporter()` zero-config
// constructor. Equivalent to the zero value LambdaLogExporter{}; provided
// for callers who prefer an explicit constructor matching every other
// exporter's own future NewXxxExporter(...) convention.
func NewLambdaLogExporter() *LambdaLogExporter {
	return &LambdaLogExporter{}
}

// Export writes record as a single line of JSON to the configured
// Writer (os.Stdout if unset).
func (e *LambdaLogExporter) Export(_ context.Context, record WorkflowInsightRecord) error {
	w := e.Writer
	if w == nil {
		w = os.Stdout
	}
	b, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.LambdaLogExporter: marshaling record: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("insight.LambdaLogExporter: writing record: %w", err)
	}
	if _, err := w.Write([]byte("\n")); err != nil {
		return fmt.Errorf("insight.LambdaLogExporter: writing record: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes
// or DefaultMaxRecordSizeBytesLog when unset.
func (e *LambdaLogExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesLog
}
