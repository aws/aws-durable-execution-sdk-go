package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeDynamoDBAPI is a test double for DynamoDBAPI.
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
		t.Fatalf("expected item to contain key %q", key)
	}
	s, ok := v.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("expected key %q to be a string attribute, got %T", key, v)
	}
	return s.Value
}

// attrJSON converts an item attribute back to the value encoding/json
// produces when it decodes JSON with UseNumber. It fails the test on an
// attribute type that JSON cannot produce, such as a set or binary.
func attrJSON(t *testing.T, av ddbtypes.AttributeValue) any {
	t.Helper()
	switch v := av.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return v.Value
	case *ddbtypes.AttributeValueMemberN:
		return json.Number(v.Value)
	case *ddbtypes.AttributeValueMemberBOOL:
		return v.Value
	case *ddbtypes.AttributeValueMemberNULL:
		return nil
	case *ddbtypes.AttributeValueMemberL:
		out := make([]any, len(v.Value))
		for i, elem := range v.Value {
			out[i] = attrJSON(t, elem)
		}
		return out
	case *ddbtypes.AttributeValueMemberM:
		out := make(map[string]any, len(v.Value))
		for k, elem := range v.Value {
			out[k] = attrJSON(t, elem)
		}
		return out
	default:
		t.Fatalf("unexpected attribute type %T", av)
		return nil
	}
}

func sortedKeys(m map[string]ddbtypes.AttributeValue) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// telemetry1Record is a terminal record in the shape the telemetry-1
// handler produces: one successful step named increment.
func telemetry1Record() Record {
	start := time.Date(2026, 11, 13, 18, 0, 0, 0, time.UTC)
	opStart := start.Add(5 * time.Millisecond)
	opEnd := opStart.Add(3 * time.Millisecond)
	end := start.Add(20 * time.Millisecond)
	execDur, opDur := int64(20), int64(3)
	return Record{
		RecordType:        RecordType,
		SchemaVersion:     SchemaVersion,
		ExecutionArn:      "arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/exec-1/inv-1",
		ExecutionName:     "exec-1",
		FunctionName:      "fn",
		FunctionQualifier: "$LATEST",
		Region:            "us-west-2",
		AccountID:         "123456789012",
		Status:            StatusSucceeded,
		StartTimestamp:    &start,
		EndTimestamp:      &end,
		DurationMs:        &execDur,
		EmittedAt:         end.Add(time.Millisecond),
		Input:             json.RawMessage(`{"value":41}`),
		Output:            json.RawMessage(`{"value":42}`),
		Operations: []OperationRecord{{
			ID: "op-1", Name: "increment", Type: "STEP", SubType: "Step", Status: "SUCCEEDED",
			StartTimestamp: &opStart, EndTimestamp: &opEnd, DurationMs: &opDur, Attempt: 1,
		}},
	}
}

// TestDynamoDBExporter_ItemIsByNameRecord checks that the item holds one
// attribute per top-level key of the by-name record JSON, plus the two key
// attributes, and nothing else.
func TestDynamoDBExporter_ItemIsByNameRecord(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry"}
	rec := telemetry1Record()

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	item := fake.calls[0].Item

	wantKeys := []string{
		"accountId", "durationMs", "emittedAt", "endTime", "executionArn",
		"executionName", "functionName", "functionQualifier", "input",
		"operationsByName", "output", "pk", "recordType", "region",
		"schemaVersion", "sk", "startTime", "status",
	}
	if got := sortedKeys(item); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("item keys:\n got %v\nwant %v", got, wantKeys)
	}

	rendered, err := RenderRecord(rec, OperationsFormatByName)
	if err != nil {
		t.Fatalf("RenderRecord: %v", err)
	}
	want := decodeWithNumbers(t, string(rendered))
	want["pk"] = rec.ExecutionArn
	want["sk"] = "2026-11-13T18:00:00.021Z"

	got := attrJSON(t, &ddbtypes.AttributeValueMemberM{Value: item})
	if !reflect.DeepEqual(got, any(want)) {
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		t.Fatalf("item does not match the by-name record:\n got %s\nwant %s", gotJSON, wantJSON)
	}
}

// TestDynamoDBExporter_AttributeTypes checks the attribute type each JSON
// value becomes, at the top level and nested inside input and
// operationsByName.
func TestDynamoDBExporter_AttributeTypes(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry"}
	rec := telemetry1Record()
	// 9007199254740993 is 2^53 + 1. A float64 cannot hold it, so the N
	// value proves the digits come from the JSON text unchanged.
	rec.Input = json.RawMessage(`{"s":"x","empty":"","big":9007199254740993,"frac":0.25,` +
		`"t":true,"f":false,"null":null,"list":[1,"a",null],"emptyList":[],"emptyObject":{}}`)
	rec.Output = json.RawMessage(`"done"`)

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	item := fake.calls[0].Item

	input, ok := item["input"].(*ddbtypes.AttributeValueMemberM)
	if !ok {
		t.Fatalf("input: want M, got %T", item["input"])
	}
	in := input.Value
	checks := []struct {
		key  string
		want ddbtypes.AttributeValue
	}{
		{"s", &ddbtypes.AttributeValueMemberS{Value: "x"}},
		{"empty", &ddbtypes.AttributeValueMemberS{Value: ""}},
		{"big", &ddbtypes.AttributeValueMemberN{Value: "9007199254740993"}},
		{"frac", &ddbtypes.AttributeValueMemberN{Value: "0.25"}},
		{"t", &ddbtypes.AttributeValueMemberBOOL{Value: true}},
		{"f", &ddbtypes.AttributeValueMemberBOOL{Value: false}},
		{"null", &ddbtypes.AttributeValueMemberNULL{Value: true}},
		{"list", &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
			&ddbtypes.AttributeValueMemberN{Value: "1"},
			&ddbtypes.AttributeValueMemberS{Value: "a"},
			&ddbtypes.AttributeValueMemberNULL{Value: true},
		}}},
		{"emptyList", &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{}}},
		{"emptyObject", &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{}}},
	}
	for _, c := range checks {
		if got := in[c.key]; !reflect.DeepEqual(got, c.want) {
			t.Errorf("input.%s = %#v, want %#v", c.key, got, c.want)
		}
	}

	if got := attrS(t, item, "output"); got != "done" {
		t.Errorf("output = %q, want the JSON string as an S attribute", got)
	}
	if v, ok := item["durationMs"].(*ddbtypes.AttributeValueMemberN); !ok || v.Value != "20" {
		t.Errorf("durationMs = %#v, want N 20", item["durationMs"])
	}

	byName, ok := item["operationsByName"].(*ddbtypes.AttributeValueMemberM)
	if !ok {
		t.Fatalf("operationsByName: want M, got %T", item["operationsByName"])
	}
	inc, ok := byName.Value["increment"].(*ddbtypes.AttributeValueMemberM)
	if !ok {
		t.Fatalf("operationsByName.increment: want M, got %T", byName.Value["increment"])
	}
	for key, want := range map[string]string{"count": "1", "maxAttempt": "1", "minDurationMs": "3"} {
		if v, ok := inc.Value[key].(*ddbtypes.AttributeValueMemberN); !ok || v.Value != want {
			t.Errorf("operationsByName.increment.%s = %#v, want N %s", key, inc.Value[key], want)
		}
	}
	if got := attrS(t, inc.Value, "subType"); got != "Step" {
		t.Errorf("operationsByName.increment.subType = %q, want Step", got)
	}
}

// TestDynamoDBExporter_KeyAttributesReplaceRecordAttributes checks that a
// key attribute named like a record key holds the key value. The
// JavaScript exporter assigns the keys after spreading the record, with
// the same result.
func TestDynamoDBExporter_KeyAttributesReplaceRecordAttributes(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	sk := "status"
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry", PartitionKeyField: "executionName", SortKeyField: &sk}
	rec := telemetry1Record()

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	item := fake.calls[0].Item
	if got := attrS(t, item, "executionName"); got != rec.ExecutionArn {
		t.Errorf("executionName = %q, want the partition key value %q", got, rec.ExecutionArn)
	}
	if got := attrS(t, item, "status"); got != "2026-11-13T18:00:00.021Z" {
		t.Errorf("status = %q, want the sort key value", got)
	}
	if _, present := item["pk"]; present {
		t.Error("expected no pk attribute when PartitionKeyField is set")
	}
}

func TestDynamoDBExporter_Render_IsByNameRecord(t *testing.T) {
	var exp RenderingSizeLimiter = &DynamoDBExporter{}
	rec := telemetry1Record()

	got, err := exp.Render(rec)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want, err := RenderRecord(rec, OperationsFormatByName)
	if err != nil {
		t.Fatalf("RenderRecord: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Render:\n got %s\nwant %s", got, want)
	}
}

// repeatedStepRecord has n operations that share one name, each with a
// result. The array rendering carries every operation and result. The
// by-name rendering carries one summary and no result, because a repeated
// name has no single representative result. So the by-name rendering is
// much smaller.
func repeatedStepRecord(n int) Record {
	rec := telemetry1Record()
	rec.Operations = nil
	result := json.RawMessage(`"` + strings.Repeat("r", 100) + `"`)
	for i := 0; i < n; i++ {
		rec.Operations = append(rec.Operations, OperationRecord{
			ID: "op-" + strconv.Itoa(i), Name: "poll", Type: "STEP", Status: "SUCCEEDED", Attempt: 1, Result: result,
		})
	}
	return rec
}

// TestDynamoDBExporter_SizeLimit_SizesByNameRendering sends a record whose
// array rendering exceeds the limit and whose by-name rendering fits. The
// plugin must size the record with the exporter's Render, so it leaves
// the record unchanged.
func TestDynamoDBExporter_SizeLimit_SizesByNameRendering(t *testing.T) {
	rec := repeatedStepRecord(200)
	byName, err := RenderRecord(rec, OperationsFormatByName)
	if err != nil {
		t.Fatalf("RenderRecord: %v", err)
	}
	array, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	limit := len(byName) + 10
	if len(array) <= limit {
		t.Fatalf("test setup: array rendering %d bytes must exceed the limit %d", len(array), limit)
	}

	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry", MaxSizeBytes: limit}
	(&TelemetryPlugin{exporters: []Exporter{exp}}).dispatch(context.Background(), rec)

	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 PutItem call, got %d", len(fake.calls))
	}
	item := fake.calls[0].Item
	if _, present := item["truncated"]; present {
		t.Error("expected no truncated attribute: the by-name rendering fits the limit")
	}
	summaries, ok := item["operationsByName"].(*ddbtypes.AttributeValueMemberM)
	if !ok {
		t.Fatalf("operationsByName: want M, got %T", item["operationsByName"])
	}
	poll, ok := summaries.Value["poll"].(*ddbtypes.AttributeValueMemberM)
	if !ok {
		t.Fatalf("operationsByName.poll: want M, got %T (operations were dropped)", summaries.Value["poll"])
	}
	if v, ok := poll.Value["count"].(*ddbtypes.AttributeValueMemberN); !ok || v.Value != "200" {
		t.Errorf("operationsByName.poll.count = %#v, want N 200 (no operation dropped)", poll.Value["count"])
	}
}

// TestDynamoDBExporter_SizeLimit_TruncatesByNameRendering sends a record
// whose by-name rendering exceeds the limit because of its input. The
// plugin drops the input, and the item records the truncation.
func TestDynamoDBExporter_SizeLimit_TruncatesByNameRendering(t *testing.T) {
	rec := telemetry1Record()
	rec.Input = json.RawMessage(`"` + strings.Repeat("i", 4096) + `"`)
	byName, err := RenderRecord(rec, OperationsFormatByName)
	if err != nil {
		t.Fatalf("RenderRecord: %v", err)
	}

	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry", MaxSizeBytes: len(byName) - 1000}
	(&TelemetryPlugin{exporters: []Exporter{exp}}).dispatch(context.Background(), rec)

	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 PutItem call, got %d", len(fake.calls))
	}
	item := fake.calls[0].Item
	if _, present := item["input"]; present {
		t.Error("expected the input attribute to be dropped")
	}
	for _, key := range []string{"truncated", "droppedInput"} {
		if v, ok := item[key].(*ddbtypes.AttributeValueMemberBOOL); !ok || !v.Value {
			t.Errorf("%s = %#v, want BOOL true", key, item[key])
		}
	}
	// Output is dropped only after input, so it is still present.
	if _, ok := item["output"].(*ddbtypes.AttributeValueMemberM); !ok {
		t.Errorf("output = %#v, want the M attribute to be kept", item["output"])
	}
}

func TestDynamoDBExporter_WithSortKey_UsesExecutionArnAndEmittedAt(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	sk := "sk"
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry", PartitionKeyField: "pk", SortKeyField: &sk}

	emittedAt := time.Date(2026, 6, 16, 17, 0, 27, 0, time.UTC)
	rec := Record{
		RecordType: RecordType, SchemaVersion: SchemaVersion,
		ExecutionArn: "arn:aws:lambda:us-east-1:111122223333:function:f:1",
		EmittedAt:    emittedAt,
		Status:       StatusSucceeded,
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 PutItem call, got %d", len(fake.calls))
	}

	item := fake.calls[0].Item
	if got := attrS(t, item, "pk"); got != rec.ExecutionArn {
		t.Errorf("expected pk=%q, got %q", rec.ExecutionArn, got)
	}
	if got := attrS(t, item, "sk"); got != "2026-06-16T17:00:27.000Z" {
		t.Errorf("expected sk=2026-06-16T17:00:27.000Z, got %q", got)
	}
	if got := attrS(t, item, "status"); got != "SUCCEEDED" {
		t.Errorf("expected status=SUCCEEDED, got %q", got)
	}
}

func TestDynamoDBExporter_NoSortKey_OmitsSortKeyAttribute(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry", PartitionKeyField: "pk", SortKeyField: NoSortKey}

	rec := Record{ExecutionArn: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if _, present := fake.calls[0].Item["sk"]; present {
		t.Error("expected no sk attribute when SortKeyField is NoSortKey")
	}
	if _, present := fake.calls[0].Item["pk"]; !present {
		t.Error("expected a pk attribute to still be present")
	}
}

func TestDynamoDBExporter_DurationMs_UsesNumberAttribute(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry", PartitionKeyField: "pk"}

	dur := int64(5414)
	rec := Record{ExecutionArn: "arn:test", DurationMs: &dur}
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
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when TableName is unset")
	}
}

func TestDynamoDBExporter_PutItemError_Propagates(t *testing.T) {
	wantErr := errors.New("throughput exceeded")
	fake := &fakeDynamoDBAPI{err: wantErr}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry"}

	err := exp.Export(context.Background(), Record{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying PutItem error to propagate (wrapped), got %v", err)
	}
}

func TestDynamoDBExporter_DefaultPartitionKey(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry"}

	rec := Record{ExecutionArn: "arn:test"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if _, present := fake.calls[0].Item["pk"]; !present {
		t.Error("expected Export to default an empty PartitionKeyField to \"pk\"")
	}
}

func TestDynamoDBExporter_MaxRecordSizeBytes(t *testing.T) {
	exp := &DynamoDBExporter{}
	if got := exp.MaxRecordSizeBytes(); got != DefaultMaxRecordSizeBytesDynamoDB {
		t.Errorf("expected %d, got %d", DefaultMaxRecordSizeBytesDynamoDB, got)
	}
	exp.MaxSizeBytes = 100
	if got := exp.MaxRecordSizeBytes(); got != 100 {
		t.Errorf("expected 100, got %d", got)
	}
}
