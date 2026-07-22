package insight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeDynamoDBAPI is a test double for DynamoDBAPI, matching
// pkg/durable/awssdk's own fakeLambdaAPI convention.
type fakeDynamoDBAPI struct {
	calls []*dynamodb.PutItemInput
	err   error
}

func (f *fakeDynamoDBAPI) PutItem(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.PutItemOutput{}, nil
}

func attrS(t *testing.T, item map[string]ddbtypes.AttributeValue, key string) string {
	t.Helper()
	v, ok := item[key]
	if !ok {
		t.Fatalf("expected item to contain key %q, item: %+v", key, item)
	}
	s, ok := v.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("expected key %q to be a string attribute, got %T", key, v)
	}
	return s.Value
}

func TestDynamoDBExporter_WithSortKey_UsesExecutionARNAndEmittedAt(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	sk := "sk"
	exp := &DynamoDBExporter{API: fake, TableName: "insight", PartitionKey: "pk", SortKey: &sk}

	emittedAt := time.Date(2026, 6, 16, 17, 0, 27, 0, time.UTC)
	rec := WorkflowInsightRecord{
		RecordType: RecordType, SchemaVersion: SchemaVersion,
		ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1",
		EmittedAt:    emittedAt,
		Status:       ExecutionStatusSucceeded,
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 PutItem call, got %d", len(fake.calls))
	}

	item := fake.calls[0].Item
	if got := attrS(t, item, "pk"); got != rec.ExecutionARN {
		t.Errorf("expected pk=%q, got %q", rec.ExecutionARN, got)
	}
	if got := attrS(t, item, "sk"); got == "" {
		t.Error("expected a non-empty sk value")
	}
	if got := attrS(t, item, "status"); got != "SUCCEEDED" {
		t.Errorf("expected status=SUCCEEDED, got %q", got)
	}
}

func TestDynamoDBExporter_NoSortKey_OmitsSortKeyAttribute(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "insight", PartitionKey: "pk", SortKey: NoSortKey}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if _, present := fake.calls[0].Item["sk"]; present {
		t.Error("expected no sk attribute when SortKey is NoSortKey")
	}
	if _, present := fake.calls[0].Item["pk"]; !present {
		t.Error("expected a pk attribute to still be present")
	}
}

func TestDynamoDBExporter_RecordJSONRoundTrips(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "insight", PartitionKey: "pk"}

	rec := WorkflowInsightRecord{
		RecordType: RecordType, SchemaVersion: SchemaVersion,
		ExecutionARN: "arn:test",
		Operations:   []OperationRecord{{ID: "op1", Type: "STEP", Status: "SUCCEEDED"}},
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	recordJSON := attrS(t, fake.calls[0].Item, "recordJson")
	var decoded WorkflowInsightRecord
	if err := json.Unmarshal([]byte(recordJSON), &decoded); err != nil {
		t.Fatalf("expected recordJson to be valid JSON: %v", err)
	}
	if len(decoded.Operations) != 1 || decoded.Operations[0].ID != "op1" {
		t.Errorf("expected the full record (including operations) to round-trip, got %+v", decoded)
	}
}

func TestDynamoDBExporter_DurationMs_UsesNumberAttribute(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "insight", PartitionKey: "pk"}

	dur := int64(5414)
	rec := WorkflowInsightRecord{ExecutionARN: "arn:test", DurationMs: &dur}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	v, ok := fake.calls[0].Item["durationMs"].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		t.Fatalf("expected durationMs to be a Number attribute, got %T", fake.calls[0].Item["durationMs"])
	}
	if v.Value != "5414" {
		t.Errorf("expected durationMs=5414, got %q", v.Value)
	}
}

func TestDynamoDBExporter_MissingTableName_ReturnsError(t *testing.T) {
	exp := &DynamoDBExporter{API: &fakeDynamoDBAPI{}}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when TableName is unset")
	}
}

func TestDynamoDBExporter_PutItemError_Propagates(t *testing.T) {
	wantErr := errors.New("throughput exceeded")
	fake := &fakeDynamoDBAPI{err: wantErr}
	exp := &DynamoDBExporter{API: fake, TableName: "insight"}

	err := exp.Export(context.Background(), WorkflowInsightRecord{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying PutItem error to propagate (wrapped), got %v", err)
	}
}

func TestDynamoDBExporter_DefaultPartitionKey(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	// PartitionKey deliberately left unset - Export itself must default
	// it to "pk", not just NewDynamoDBExporter (a caller constructing
	// DynamoDBExporter directly, e.g. in a test, should get the same
	// default).
	exp := &DynamoDBExporter{API: fake, TableName: "insight"}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if _, present := fake.calls[0].Item["pk"]; !present {
		t.Error("expected Export to default an empty PartitionKey to \"pk\"")
	}
}
