package telemetry

import (
	"context"
	"fmt"
	"io"
	"os"
)

// LambdaLogExporter writes records as a single JSON line to stdout.
// Since Lambda captures stdout to the function's CloudWatch log group
// automatically, this requires zero IAM permissions and zero setup.
//
// Each line carries the record with its operations rendered as
// operationsByName (see OperationsFormatByName), never as the operations
// array. The line lands in CloudWatch Logs, where Logs Insights can read a
// field by a dotted path but cannot select an array element by its name.
// So the format is fixed and has no option, as for CloudWatchLogsExporter
// and the JavaScript, Python, and Java LambdaLogExporter.
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
// (os.Stdout if unset). The line is Render's output followed by a newline.
func (e *LambdaLogExporter) Export(_ context.Context, record Record) error {
	w := e.Writer
	if w == nil {
		w = os.Stdout
	}
	b, err := e.Render(record)
	if err != nil {
		return fmt.Errorf("telemetry.LambdaLogExporter: marshaling record: %w", err)
	}
	b = append(b, '\n')
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("telemetry.LambdaLogExporter: writing record: %w", err)
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

// Render implements RenderingSizeLimiter. It returns the line Export
// writes, without the trailing newline: the record rendered with
// OperationsFormatByName. Export calls Render, so truncation sizes the
// bytes that are written.
func (e *LambdaLogExporter) Render(record Record) ([]byte, error) {
	return RenderRecord(record, OperationsFormatByName)
}
