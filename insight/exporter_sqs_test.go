package insight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// fakeSQSAPI is a test double for SQSAPI.
type fakeSQSAPI struct {
	calls []*sqs.SendMessageInput
	err   error
}

func (f *fakeSQSAPI) SendMessage(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &sqs.SendMessageOutput{}, nil
}

func TestSQSExporter_StandardQueue_NoFIFOFields(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/insight-queue"}

	rec := Record{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionArn: "arn:test", Status: StatusSucceeded, FunctionName: "f"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 SendMessage call, got %d", len(fake.calls))
	}
	call := fake.calls[0]
	if call.MessageGroupId != nil {
		t.Errorf("expected no MessageGroupId for a standard queue, got %q", aws.ToString(call.MessageGroupId))
	}
	if call.MessageDeduplicationId != nil {
		t.Errorf("expected no MessageDeduplicationId for a standard queue, got %q", aws.ToString(call.MessageDeduplicationId))
	}
}

func TestSQSExporter_FIFOQueue_AutoDetectedFromURLSuffix(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/insight-queue.fifo"}

	emittedAt := time.Date(2026, 6, 16, 17, 0, 27, 0, time.UTC)
	rec := Record{ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:f:1", EmittedAt: emittedAt}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	call := fake.calls[0]
	if aws.ToString(call.MessageGroupId) != rec.ExecutionArn {
		t.Errorf("expected MessageGroupId to default to ExecutionArn=%q, got %q", rec.ExecutionArn, aws.ToString(call.MessageGroupId))
	}
	wantDedup := rec.ExecutionArn + ":" + emittedAt.Format(rfc3339Milli)
	if aws.ToString(call.MessageDeduplicationId) != wantDedup {
		t.Errorf("expected MessageDeduplicationId=%q, got %q", wantDedup, aws.ToString(call.MessageDeduplicationId))
	}
}

func TestSQSExporter_CustomMessageGroupID(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.example.com/q.fifo", MessageGroupID: "custom-group"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := aws.ToString(fake.calls[0].MessageGroupId); got != "custom-group" {
		t.Errorf("expected MessageGroupId=custom-group, got %q", got)
	}
}

func TestSQSExporter_MessageAttributesForFiltering(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.example.com/q"}

	rec := Record{ExecutionArn: "arn:test", Status: StatusFailed, FunctionName: "order-processor"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	attrs := fake.calls[0].MessageAttributes
	if got := aws.ToString(attrs["status"].StringValue); got != "FAILED" {
		t.Errorf("expected status attribute=FAILED, got %q", got)
	}
	if got := aws.ToString(attrs["functionName"].StringValue); got != "order-processor" {
		t.Errorf("expected functionName attribute=order-processor, got %q", got)
	}
}

func TestSQSExporter_MessageBodyIsValidJSON(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.example.com/q"}

	rec := Record{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionArn: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var decoded Record
	if err := json.Unmarshal([]byte(aws.ToString(fake.calls[0].MessageBody)), &decoded); err != nil {
		t.Fatalf("expected MessageBody to be valid JSON: %v", err)
	}
	if decoded.ExecutionArn != rec.ExecutionArn {
		t.Errorf("expected round-tripped ExecutionArn %q, got %q", rec.ExecutionArn, decoded.ExecutionArn)
	}
}

func TestSQSExporter_OperationsFormatByName(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.example.com/q", OperationsFormat: OperationsFormatByName}

	rec := Record{ExecutionArn: "arn:test", Operations: []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}}}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(aws.ToString(fake.calls[0].MessageBody)), &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' to be absent under OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' to be present under OperationsFormatByName")
	}
}

func TestSQSExporter_MissingQueueURL_ReturnsError(t *testing.T) {
	exp := &SQSExporter{API: &fakeSQSAPI{}}
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when QueueURL is unset")
	}
}

func TestSQSExporter_SendMessageError_Propagates(t *testing.T) {
	wantErr := errors.New("queue does not exist")
	fake := &fakeSQSAPI{err: wantErr}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.example.com/q"}

	err := exp.Export(context.Background(), Record{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying SendMessage error to propagate (wrapped), got %v", err)
	}
}
