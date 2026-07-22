package insight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Partitioning controls how S3Exporter keys objects, matching the JS
// SDK's own S3Exporter partitioning option.
type S3Partitioning string

const (
	// S3PartitioningDate keys objects under Hive-style
	// year=YYYY/month=MM/day=DD/ partitions, auto-discovered by Glue
	// crawlers and Athena's MSCK REPAIR TABLE. The default.
	S3PartitioningDate S3Partitioning = "date"

	// S3PartitioningFunctionName keys objects under the record's own
	// FunctionName instead of a date.
	S3PartitioningFunctionName S3Partitioning = "function-name"

	// S3PartitioningNone applies no partitioning prefix at all beyond
	// Prefix itself.
	S3PartitioningNone S3Partitioning = "none"
)

// S3API is the subset of *s3.Client this package depends on, exposed as
// an interface so tests can substitute a fake without needing real AWS
// credentials or network access - matching the exact convention
// pkg/durable/awssdk.LambdaAPI already established for this same reason.
type S3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Exporter writes records as JSON objects to Amazon S3, matching the JS
// SDK's own S3Exporter. Uses the record's own ExecutionName (falling
// back to a sanitized ExecutionARN when ExecutionName is empty - see
// objectKey) as the object key, so a later update to the SAME execution
// overwrites the same object rather than creating a new one each time -
// matching the JS SDK's own documented "upsert by key" behavior.
type S3Exporter struct {
	// API is the underlying S3 client. If nil, NewS3Exporter must be
	// used to construct one with a real *s3.Client resolved from the
	// standard AWS SDK credential/region chain.
	API S3API

	// Bucket is the target S3 bucket. Required.
	Bucket string

	// Prefix is prepended to every object key. Defaults to
	// "workflow-insight/" when empty, matching the JS SDK's own default.
	Prefix string

	// Partitioning controls the partition segment inserted between
	// Prefix and the object's own filename. Defaults to
	// S3PartitioningDate when left at its zero value.
	Partitioning S3Partitioning

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesS3. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesS3
	// instead.
	MaxSizeBytes int
}

// NewS3Exporter constructs an S3Exporter backed by a real *s3.Client,
// resolving credentials and region via the standard AWS SDK for Go v2
// default chain - matching pkg/durable/awssdk.New's own established
// convention.
func NewS3Exporter(ctx context.Context, bucket string, optFns ...func(*awsconfig.LoadOptions) error) (*S3Exporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewS3Exporter: loading AWS config: %w", err)
	}
	return &S3Exporter{
		API:    s3.NewFromConfig(cfg),
		Bucket: bucket,
	}, nil
}

// Export writes record as a JSON object to this exporter's bucket, at a
// key determined by Prefix/Partitioning/the record's own identity - see
// objectKey.
func (e *S3Exporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.S3Exporter: API is nil (use NewS3Exporter, or set API explicitly for tests)")
	}
	if e.Bucket == "" {
		return fmt.Errorf("insight.S3Exporter: Bucket is required")
	}

	b, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.S3Exporter: marshaling record: %w", err)
	}

	key := e.objectKey(record)
	_, err = e.API.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(e.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(b),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("insight.S3Exporter: PutObject: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesS3 when unset.
func (e *S3Exporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesS3
}

// objectKey builds this record's own S3 key: "{prefix}{partition}{name}.json",
// matching the JS SDK's own documented key pattern
// "workflow-insight/year=2026/month=06/day=16/{executionName}.json"
// exactly for the default (date) partitioning.
func (e *S3Exporter) objectKey(record WorkflowInsightRecord) string {
	prefix := e.Prefix
	if prefix == "" {
		prefix = "workflow-insight/"
	}

	partition := ""
	switch e.Partitioning {
	case S3PartitioningFunctionName:
		if record.FunctionName != "" {
			partition = record.FunctionName + "/"
		}
	case S3PartitioningNone:
		partition = ""
	default: // S3PartitioningDate, or unset (the default)
		t := record.StartTime
		if t.IsZero() {
			t = time.Now().UTC()
		}
		partition = fmt.Sprintf("year=%04d/month=%02d/day=%02d/", t.Year(), t.Month(), t.Day())
	}

	name := record.ExecutionName
	if name == "" {
		name = sanitizeForKey(record.ExecutionARN)
	}
	if name == "" {
		name = "unknown"
	}
	return prefix + partition + name + ".json"
}

// sanitizeForKey replaces characters an S3 key would rather not contain
// verbatim (colons and slashes, both present in a raw Lambda ARN) with
// underscores, so a record with no ExecutionName still gets a
// reasonably well-formed fallback key instead of one containing raw
// ARN path separators that would be misread as key "directories".
func sanitizeForKey(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == ':' || r == '/' {
			out = append(out, '_')
			continue
		}
		out = append(out, r)
	}
	return string(out)
}
