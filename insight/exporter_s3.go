package insight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Partitioning controls how S3Exporter keys objects.
type S3Partitioning string

const (
	// S3PartitioningDate keys objects under Hive-style
	// year=YYYY/month=MM/day=DD/ partitions. The default.
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
// credentials or network access.
type S3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Exporter writes records as JSON objects to Amazon S3. Uses the
// record's own ExecutionName (falling back to a sanitized ExecutionArn
// when ExecutionName is empty) as the object key, so a later update to
// the same execution overwrites the same object rather than creating a
// new one each time.
type S3Exporter struct {
	// API is the underlying S3 client. Required.
	API S3API

	// Bucket is the target S3 bucket. Required.
	Bucket string

	// Prefix is prepended to every object key. Defaults to
	// "workflow-insight/" when empty.
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

// Export writes record as a JSON object to this exporter's bucket, at a
// key determined by Prefix/Partitioning/the record's own identity.
func (e *S3Exporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.S3Exporter: API is nil")
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

// objectKey builds this record's own S3 key:
// "{prefix}{partition}{name}.json".
func (e *S3Exporter) objectKey(record Record) string {
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
	default: // S3PartitioningDate or unset
		t := record.StartTimestamp
		if t == nil {
			now := time.Now().UTC()
			t = &now
		}
		partition = fmt.Sprintf("year=%04d/month=%02d/day=%02d/", t.Year(), t.Month(), t.Day())
	}

	name := record.ExecutionName
	if name == "" {
		name = sanitizeForS3Key(record.ExecutionArn)
	}
	if name == "" {
		name = "unknown"
	}
	return prefix + partition + name + ".json"
}

// sanitizeForS3Key replaces characters an S3 key would rather not
// contain verbatim (colons and slashes) with underscores.
func sanitizeForS3Key(s string) string {
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
