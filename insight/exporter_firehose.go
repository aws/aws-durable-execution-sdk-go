package insight

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
)

// FirehoseAPI is the subset of *firehose.Client this package depends on,
// exposed as an interface so tests can substitute a fake without needing
// real AWS credentials or network access.
type FirehoseAPI interface {
	PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

// FirehoseExporter sends records to Amazon Kinesis Data Firehose for
// delivery to S3, Redshift, Splunk, or any HTTP endpoint the delivery
// stream is configured with.
//
// Each record is appended with a trailing newline (NDJSON), so records
// remain individually parseable once Firehose batches multiple PutRecord
// calls into a single S3 object.
type FirehoseExporter struct {
	// API is the underlying Firehose client. Required.
	API FirehoseAPI

	// DeliveryStreamName is the target delivery stream. Required.
	DeliveryStreamName string

	// OperationsFormat controls how operations are rendered. Defaults to
	// OperationsFormatArray when left empty.
	OperationsFormat OperationsFormat

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesFirehose. Left at
	// its zero value, MaxRecordSizeBytes() returns
	// DefaultMaxRecordSizeBytesFirehose instead.
	MaxSizeBytes int
}

// Export sends record as a single NDJSON-formatted PutRecord call.
func (e *FirehoseExporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.FirehoseExporter: API is nil")
	}
	if e.DeliveryStreamName == "" {
		return fmt.Errorf("insight.FirehoseExporter: DeliveryStreamName is required")
	}

	b, err := RenderRecord(record, e.OperationsFormat)
	if err != nil {
		return fmt.Errorf("insight.FirehoseExporter: rendering record: %w", err)
	}
	b = append(b, '\n')

	_, err = e.API.PutRecord(ctx, &firehose.PutRecordInput{
		DeliveryStreamName: aws.String(e.DeliveryStreamName),
		Record:             &firehosetypes.Record{Data: b},
	})
	if err != nil {
		return fmt.Errorf("insight.FirehoseExporter: PutRecord: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesFirehose when unset.
func (e *FirehoseExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesFirehose
}

// Render implements RenderingSizeLimiter, returning the exact bytes
// Export would send as Record.Data (including the trailing NDJSON
// newline).
func (e *FirehoseExporter) Render(record Record) ([]byte, error) {
	b, err := RenderRecord(record, e.OperationsFormat)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
