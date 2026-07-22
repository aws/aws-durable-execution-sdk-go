package insight

import (
	"context"
)

// Exporter is the interface a Workflow Insight destination implements,
// matching the JS SDK's own InsightExporter.
//
// Export is called with the record whenever a new snapshot is ready to
// send, per the configured EmitMode. Export errors are swallowed by the
// plugin (never fail the execution or affect other configured
// exporters) - matching the JS SDK's own documented "exporter errors
// never fail the execution" contract, and this SDK's own broader
// plugin.Dispatch precedent of never letting instrumentation failures
// affect execution correctness.
type Exporter interface {
	Export(ctx context.Context, record WorkflowInsightRecord) error
}

// Flusher is an optional interface an Exporter may additionally
// implement to flush any buffered data before the invocation returns,
// matching the JS SDK's own optional InsightExporter.flush(). Checked via
// a type assertion at dispatch time rather than being part of the
// required Exporter interface itself, so an exporter with nothing to
// flush (like LambdaLogExporter, which writes synchronously) does not
// need a no-op method.
type Flusher interface {
	Flush(ctx context.Context) error
}

// SizeLimiter is an optional interface an Exporter may additionally
// implement to opt into size-based truncation, matching the JS SDK's
// own optional InsightExporter.maxRecordSizeBytes property.
//
// When an Exporter implements SizeLimiter, Plugin.dispatch (see
// truncate.go) sends that exporter its OWN, INDEPENDENTLY truncated
// copy of the record - best-effort, per the JS SDK's own documented drop
// order (see Truncate's own doc) - if the record's own serialized JSON
// size exceeds MaxRecordSizeBytes(). Different exporters configured on
// the SAME Plugin can therefore receive DIFFERENT copies of the very
// same logical record: one exporter's own smaller limit may truncate
// data another, larger-limited exporter receives in full - matching the
// JS SDK's own documented "the same record can go out full to one
// exporter and trimmed to another" behavior exactly.
//
// An Exporter that does NOT implement SizeLimiter (or whose
// MaxRecordSizeBytes() returns <= 0) receives the record UNTRUNCATED,
// regardless of its actual size - matching the JS SDK's own documented
// "no default - truncation is disabled unless you set
// maxRecordSizeBytes" behavior for HttpExporter/FileExporter
// specifically, and this Go port's own DefaultMaxRecordSizeBytes*
// constants for every other built-in exporter (see truncate.go).
type SizeLimiter interface {
	MaxRecordSizeBytes() int
}

// RenderingSizeLimiter is SizeLimiter's own companion interface for an
// exporter that ALSO supports OperationsFormat (Firehose, EventBridge,
// SQS, OTel, HTTP, File - see that type's own doc): Render must return
// the EXACT byte shape this exporter's own Export call would actually
// send, so Plugin.dispatch's own truncation sizes against the real
// rendered form (see Truncate's own Renderer parameter for why this
// matters - an exporter using OperationsFormatByName/
// OperationsFormatBoth renders a differently-sized body than the
// canonical array shape).
//
// An exporter that implements SizeLimiter but NOT RenderingSizeLimiter
// (every exporter with no OperationsFormat option at all - LambdaLog,
// CloudWatchLogs, S3, DynamoDB, Aurora, Redshift, OpenSearch) is sized
// against the canonical WorkflowInsightRecord JSON shape instead (see
// Truncate's own defaultRenderer) - correct for all of them, since none
// of them have a second rendering to consider.
type RenderingSizeLimiter interface {
	SizeLimiter
	Render(record WorkflowInsightRecord) ([]byte, error)
}
