package insight

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
)

// fakeFirehoseAPI is a test double for FirehoseAPI.
type fakeFirehoseAPI struct {
	calls []*firehose.PutRecordInput
	err   error
}

func (f *fakeFirehoseAPI) PutRecord(_ context.Context, params *firehose.PutRecordInput, _ ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &firehose.PutRecordOutput{}, nil
}

func TestFirehoseExporter_SendsNDJSONWithTrailingNewline(t *testing.T) {
	fake := &fakeFirehoseAPI{}
	exp := &FirehoseExporter{API: fake, DeliveryStreamName: "insight-stream"}

	rec := Record{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionArn: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 PutRecord call, got %d", len(fake.calls))
	}
	if aws.ToString(fake.calls[0].DeliveryStreamName) != "insight-stream" {
		t.Errorf("expected DeliveryStreamName=insight-stream, got %q", aws.ToString(fake.calls[0].DeliveryStreamName))
	}
	data := fake.calls[0].Record.Data
	if !strings.HasSuffix(string(data), "\n") {
		t.Fatalf("expected a trailing newline (NDJSON), got %q", string(data))
	}
	var decoded Record
	if err := json.Unmarshal(data[:len(data)-1], &decoded); err != nil {
		t.Fatalf("expected the data (minus trailing newline) to be valid JSON: %v", err)
	}
	if decoded.ExecutionArn != rec.ExecutionArn {
		t.Errorf("expected round-tripped ExecutionArn %q, got %q", rec.ExecutionArn, decoded.ExecutionArn)
	}
}

func TestFirehoseExporter_OperationsFormatByName(t *testing.T) {
	fake := &fakeFirehoseAPI{}
	exp := &FirehoseExporter{API: fake, DeliveryStreamName: "insight-stream", OperationsFormat: OperationsFormatByName}

	rec := Record{
		ExecutionArn: "arn:test",
		Operations:   []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}},
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	data := fake.calls[0].Record.Data
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data[:len(data)-1], &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' to be absent under OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' to be present under OperationsFormatByName")
	}
}

func TestFirehoseExporter_MissingDeliveryStreamName_ReturnsError(t *testing.T) {
	exp := &FirehoseExporter{API: &fakeFirehoseAPI{}}
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when DeliveryStreamName is unset")
	}
}

func TestFirehoseExporter_PutRecordError_Propagates(t *testing.T) {
	wantErr := errors.New("throttled")
	fake := &fakeFirehoseAPI{err: wantErr}
	exp := &FirehoseExporter{API: fake, DeliveryStreamName: "insight-stream"}

	err := exp.Export(context.Background(), Record{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying PutRecord error to propagate (wrapped), got %v", err)
	}
}
