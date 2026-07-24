package insight

import "context"

// Exporter is the interface a Workflow Insight destination implements.
// Export is called with the record whenever a new snapshot is ready to
// send, per the configured EmitMode.
//
// Export errors are swallowed by the plugin (never fail the execution or
// affect other configured exporters).
type Exporter interface {
	Export(ctx context.Context, record Record) error
}

// Flusher is an optional interface an Exporter may additionally
// implement to flush any buffered data before the invocation returns.
// Checked via a type assertion at dispatch time rather than being part
// of the required Exporter interface.
type Flusher interface {
	Flush(ctx context.Context) error
}

// SizeLimiter is an optional interface an Exporter may additionally
// implement to opt into size-based truncation. When an Exporter
// implements SizeLimiter, the plugin sends that exporter its own,
// independently truncated copy of the record.
type SizeLimiter interface {
	MaxRecordSizeBytes() int
}

// RenderingSizeLimiter is SizeLimiter's companion interface for an
// exporter that also supports OperationsFormat. Render must return the
// exact byte shape this exporter's Export call would actually send, so
// truncation sizes against the real rendered form.
type RenderingSizeLimiter interface {
	SizeLimiter
	Render(record Record) ([]byte, error)
}

// DefaultMaxRecordSizeBytesLog is the default maximum record size in
// bytes for the LambdaLogExporter (256 KB minus overhead).
const DefaultMaxRecordSizeBytesLog = 256 * 1024

// DefaultMaxRecordSizeBytesS3 is the default maximum record size in
// bytes for the S3Exporter (5 MB).
const DefaultMaxRecordSizeBytesS3 = 5 * 1024 * 1024

// DefaultMaxRecordSizeBytesFirehose is the default maximum record size
// in bytes for the FirehoseExporter (1 MB).
const DefaultMaxRecordSizeBytesFirehose = 1 * 1024 * 1024
