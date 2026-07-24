package insight

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeS3API is a test double for S3API.
type fakeS3API struct {
	calls    []*s3.PutObjectInput
	lastBody []byte
	err      error
}

func (f *fakeS3API) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, _ := io.ReadAll(params.Body)
	captured := *params
	captured.Body = nil
	f.calls = append(f.calls, &captured)
	f.lastBody = body
	if f.err != nil {
		return nil, f.err
	}
	return &s3.PutObjectOutput{}, nil
}

func TestS3Exporter_DatePartitioning_DefaultKeyPattern(t *testing.T) {
	fake := &fakeS3API{}
	exp := &S3Exporter{API: fake, Bucket: "my-bucket"}

	startTime := time.Date(2026, 6, 16, 17, 0, 22, 0, time.UTC)
	rec := Record{
		RecordType: RecordType, SchemaVersion: SchemaVersion,
		ExecutionName:  "abc123",
		StartTimestamp: &startTime,
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 PutObject call, got %d", len(fake.calls))
	}
	got := aws.ToString(fake.calls[0].Key)
	want := "workflow-insight/year=2026/month=06/day=16/abc123.json"
	if got != want {
		t.Errorf("expected key %q, got %q", want, got)
	}
	if aws.ToString(fake.calls[0].Bucket) != "my-bucket" {
		t.Errorf("expected bucket=my-bucket, got %q", aws.ToString(fake.calls[0].Bucket))
	}
}

func TestS3Exporter_FunctionNamePartitioning(t *testing.T) {
	fake := &fakeS3API{}
	exp := &S3Exporter{API: fake, Bucket: "my-bucket", Partitioning: S3PartitioningFunctionName}

	rec := Record{ExecutionName: "exec-1", FunctionName: "order-processor"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	want := "workflow-insight/order-processor/exec-1.json"
	if got := aws.ToString(fake.calls[0].Key); got != want {
		t.Errorf("expected key %q, got %q", want, got)
	}
}

func TestS3Exporter_NoPartitioning(t *testing.T) {
	fake := &fakeS3API{}
	exp := &S3Exporter{API: fake, Bucket: "my-bucket", Partitioning: S3PartitioningNone}

	rec := Record{ExecutionName: "exec-1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	want := "workflow-insight/exec-1.json"
	if got := aws.ToString(fake.calls[0].Key); got != want {
		t.Errorf("expected key %q, got %q", want, got)
	}
}

func TestS3Exporter_FallsBackToSanitizedARNWhenNoExecutionName(t *testing.T) {
	fake := &fakeS3API{}
	exp := &S3Exporter{API: fake, Bucket: "my-bucket", Partitioning: S3PartitioningNone}

	rec := Record{ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:f:1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	got := aws.ToString(fake.calls[0].Key)
	if got == "workflow-insight/.json" || got == "workflow-insight/unknown.json" {
		t.Errorf("expected the sanitized ARN to be used as a fallback key, got %q", got)
	}
	filename := got[len("workflow-insight/") : len(got)-len(".json")]
	if strings.ContainsAny(filename, ":/") {
		t.Errorf("expected the ARN-derived filename to have ':' and '/' sanitized out, got %q", filename)
	}
}

func TestS3Exporter_BodyIsValidJSON(t *testing.T) {
	fake := &fakeS3API{}
	exp := &S3Exporter{API: fake, Bucket: "my-bucket"}

	rec := Record{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionArn: "arn:test", ExecutionName: "e1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var decoded Record
	if err := json.Unmarshal(fake.lastBody, &decoded); err != nil {
		t.Fatalf("expected the PutObject body to be valid JSON: %v", err)
	}
	if decoded.ExecutionArn != rec.ExecutionArn {
		t.Errorf("expected round-tripped ExecutionArn %q, got %q", rec.ExecutionArn, decoded.ExecutionArn)
	}
	if aws.ToString(fake.calls[0].ContentType) != "application/json" {
		t.Errorf("expected ContentType=application/json, got %q", aws.ToString(fake.calls[0].ContentType))
	}
}

func TestS3Exporter_MissingBucket_ReturnsError(t *testing.T) {
	exp := &S3Exporter{API: &fakeS3API{}}
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when Bucket is unset")
	}
}

func TestS3Exporter_PutObjectError_Propagates(t *testing.T) {
	wantErr := errors.New("access denied")
	fake := &fakeS3API{err: wantErr}
	exp := &S3Exporter{API: fake, Bucket: "my-bucket"}

	err := exp.Export(context.Background(), Record{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying PutObject error to propagate (wrapped), got %v", err)
	}
}
