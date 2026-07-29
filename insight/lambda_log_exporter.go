package insight

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// LambdaLogExporter writes records as a single JSON line to stdout.
// Since Lambda captures stdout to the function's CloudWatch log group
// automatically, this requires zero IAM permissions and zero setup.
type LambdaLogExporter struct {
	// Writer overrides the default destination (os.Stdout). Primarily
	// for tests.
	Writer io.Writer

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesLog. Left at zero,
	// MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesLog.
	MaxSizeBytes int
}

// NewLambdaLogExporter returns a LambdaLogExporter writing to os.Stdout.
func NewLambdaLogExporter() *LambdaLogExporter {
	return &LambdaLogExporter{}
}

// Export writes record as a single line of JSON to the configured Writer
// (os.Stdout if unset).
func (e *LambdaLogExporter) Export(_ context.Context, record Record) error {
	w := e.Writer
	if w == nil {
		w = os.Stdout
	}
	b, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.LambdaLogExporter: marshaling record: %w", err)
	}
	b = append(b, '\n')
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("insight.LambdaLogExporter: writing record: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesLog when unset.
func (e *LambdaLogExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesLog
}
