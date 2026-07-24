package insight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// fakeEventBridgeAPI is a test double for EventBridgeAPI.
type fakeEventBridgeAPI struct {
	calls            []*eventbridge.PutEventsInput
	err              error
	failedEntryCount int32
	entryErrorCode   string
	entryErrorMsg    string
}

func (f *fakeEventBridgeAPI) PutEvents(_ context.Context, params *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	out := &eventbridge.PutEventsOutput{FailedEntryCount: f.failedEntryCount}
	if f.failedEntryCount > 0 {
		out.Entries = []ebtypes.PutEventsResultEntry{{
			ErrorCode:    aws.String(f.entryErrorCode),
			ErrorMessage: aws.String(f.entryErrorMsg),
		}}
	}
	return out, nil
}

func TestEventBridgeExporter_DefaultsSourceAndEventBus(t *testing.T) {
	fake := &fakeEventBridgeAPI{}
	exp := &EventBridgeExporter{API: fake}

	rec := Record{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionArn: "arn:test", Status: StatusSucceeded}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 || len(fake.calls[0].Entries) != 1 {
		t.Fatalf("expected exactly 1 PutEvents call with 1 entry, got %d calls", len(fake.calls))
	}
	entry := fake.calls[0].Entries[0]
	if aws.ToString(entry.Source) != DefaultEventBridgeSource {
		t.Errorf("expected default Source %q, got %q", DefaultEventBridgeSource, aws.ToString(entry.Source))
	}
	if aws.ToString(entry.EventBusName) != "default" {
		t.Errorf("expected default EventBusName 'default', got %q", aws.ToString(entry.EventBusName))
	}
	if aws.ToString(entry.DetailType) != "SUCCEEDED" {
		t.Errorf("expected DetailType=SUCCEEDED, got %q", aws.ToString(entry.DetailType))
	}
}

func TestEventBridgeExporter_CustomSourceAndBus(t *testing.T) {
	fake := &fakeEventBridgeAPI{}
	exp := &EventBridgeExporter{API: fake, Source: "my.custom.source", EventBusName: "my-bus"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test", Status: StatusFailed}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	entry := fake.calls[0].Entries[0]
	if aws.ToString(entry.Source) != "my.custom.source" {
		t.Errorf("expected Source=my.custom.source, got %q", aws.ToString(entry.Source))
	}
	if aws.ToString(entry.EventBusName) != "my-bus" {
		t.Errorf("expected EventBusName=my-bus, got %q", aws.ToString(entry.EventBusName))
	}
	if aws.ToString(entry.DetailType) != "FAILED" {
		t.Errorf("expected DetailType=FAILED, got %q", aws.ToString(entry.DetailType))
	}
}

func TestEventBridgeExporter_DetailIsFullRecordJSON(t *testing.T) {
	fake := &fakeEventBridgeAPI{}
	exp := &EventBridgeExporter{API: fake}

	rec := Record{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:f:1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	detail := aws.ToString(fake.calls[0].Entries[0].Detail)
	var decoded Record
	if err := json.Unmarshal([]byte(detail), &decoded); err != nil {
		t.Fatalf("expected Detail to be valid JSON: %v", err)
	}
	if decoded.ExecutionArn != rec.ExecutionArn {
		t.Errorf("expected round-tripped ExecutionArn %q, got %q", rec.ExecutionArn, decoded.ExecutionArn)
	}
}

func TestEventBridgeExporter_OperationsFormatByName(t *testing.T) {
	fake := &fakeEventBridgeAPI{}
	exp := &EventBridgeExporter{API: fake, OperationsFormat: OperationsFormatByName}

	rec := Record{ExecutionArn: "arn:test", Operations: []OperationRecord{{Name: "s", Type: "STEP", Status: "SUCCEEDED"}}}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	detail := aws.ToString(fake.calls[0].Entries[0].Detail)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(detail), &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["operations"]; ok {
		t.Error("expected 'operations' to be absent under OperationsFormatByName")
	}
	if _, ok := fields["operationsByName"]; !ok {
		t.Error("expected 'operationsByName' to be present under OperationsFormatByName")
	}
}

func TestEventBridgeExporter_PartialFailure_ReturnsError(t *testing.T) {
	fake := &fakeEventBridgeAPI{failedEntryCount: 1, entryErrorCode: "InternalFailure", entryErrorMsg: "something broke"}
	exp := &EventBridgeExporter{API: fake}

	err := exp.Export(context.Background(), Record{})
	if err == nil {
		t.Fatal("expected an error when PutEvents reports FailedEntryCount > 0")
	}
}

func TestEventBridgeExporter_PutEventsError_Propagates(t *testing.T) {
	wantErr := errors.New("access denied")
	fake := &fakeEventBridgeAPI{err: wantErr}
	exp := &EventBridgeExporter{API: fake}

	err := exp.Export(context.Background(), Record{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying PutEvents error to propagate (wrapped), got %v", err)
	}
}
