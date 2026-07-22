package insight

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
)

// FirehoseAPI is the subset of *firehose.Client this package depends on,
// exposed as an interface so tests can substitute a fake without needing
// real AWS credentials or network access - matching the exact convention
// pkg/durable/awssdk.LambdaAPI already established for this same reason.
type FirehoseAPI interface {
	PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

// FirehoseExporter sends records to Amazon Kinesis Data Firehose for
// delivery to S3, Redshift, Splunk, or any HTTP endpoint the delivery
// stream is itself configured with, matching the JS SDK's own
// FirehoseExporter.
//
// # Format
//
// Matching the JS SDK's own documented behavior: each record is appended
// with a trailing newline (NDJSON - newline-delimited JSON), so records
// remain individually parseable once Firehose batches multiple PutRecord
// calls into a single S3 object.
type FirehoseExporter struct {
	// API is the underlying Firehose client. If nil, NewFirehoseExporter
	// must be used to construct one with a real *firehose.Client
	// resolved from the standard AWS SDK credential/region chain.
	API FirehoseAPI

	// DeliveryStreamName is the target delivery stream. Required.
	DeliveryStreamName string

	// OperationsFormat controls how the record's own operations are
	// rendered - see that type's own doc. Defaults to
	// OperationsFormatArray when left at its zero value.
	OperationsFormat OperationsFormat

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesSQL. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesSQL
	// instead.
	MaxSizeBytes int
}

// NewFirehoseExporter constructs a FirehoseExporter backed by a real
// *firehose.Client, resolving credentials and region via the standard
// AWS SDK for Go v2 default chain - matching pkg/durable/awssdk.New's
// own established convention.
func NewFirehoseExporter(ctx context.Context, deliveryStreamName string, optFns ...func(*awsconfig.LoadOptions) error) (*FirehoseExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewFirehoseExporter: loading AWS config: %w", err)
	}
	return &FirehoseExporter{
		API:                firehose.NewFromConfig(cfg),
		DeliveryStreamName: deliveryStreamName,
	}, nil
}

// Export sends record as a single NDJSON-formatted PutRecord call.
func (e *FirehoseExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.FirehoseExporter: API is nil (use NewFirehoseExporter, or set API explicitly for tests)")
	}
	if e.DeliveryStreamName == "" {
		return fmt.Errorf("insight.FirehoseExporter: DeliveryStreamName is required")
	}

	b, err := renderRecord(record, e.OperationsFormat)
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
// DefaultMaxRecordSizeBytesSQL when unset.
func (e *FirehoseExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesSQL
}

// Render implements RenderingSizeLimiter, returning the EXACT bytes
// Export would send as Record.Data (including the trailing NDJSON
// newline), so Truncate's own fits-or-doesn't-fit sizing reflects the
// real serialized form under e's own OperationsFormat.
func (e *FirehoseExporter) Render(record WorkflowInsightRecord) ([]byte, error) {
	b, err := renderRecord(record, e.OperationsFormat)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
