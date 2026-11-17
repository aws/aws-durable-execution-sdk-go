package telemetry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// contentTestRecord is a record whose content fields hold JSON values of
// different types, as the plugin now stores them.
func contentTestRecord() Record {
	start := time.Date(2026, 6, 16, 17, 0, 26, 500_000_000, time.UTC)
	end := start.Add(time.Second)
	dur := end.Sub(start).Milliseconds()
	return Record{
		RecordType:     RecordType,
		SchemaVersion:  SchemaVersion,
		ExecutionArn:   "arn:test",
		Status:         StatusSucceeded,
		StartTimestamp: &start,
		EndTimestamp:   &end,
		DurationMs:     &dur,
		EmittedAt:      end,
		Input:          json.RawMessage(`{"value":41}`),
		Output:         json.RawMessage(`"done"`),
		Operations: []OperationRecord{
			{
				ID: "op-1", Name: "increment", Type: "STEP", SubType: "Step", Status: "SUCCEEDED",
				StartTimestamp: &start, EndTimestamp: &end, DurationMs: &dur, Attempt: 1,
				Result: json.RawMessage(`{"value":42}`),
			},
			{
				ID: "op-2", Name: "count", Type: "STEP", SubType: "Step", Status: "SUCCEEDED",
				StartTimestamp: &start, EndTimestamp: &end, DurationMs: &dur, Attempt: 1,
				Result: json.RawMessage(`7`),
			},
		},
	}
}

// assertContentValues checks that a rendered record carries input, output,
// and operation results as JSON values, not {value: "..."} wrappers.
func assertContentValues(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not valid JSON: %v\n%s", err, body)
	}
	if in, ok := decoded["input"].(map[string]any); !ok || in["value"] != float64(41) {
		t.Errorf("input = %v, want {\"value\": 41}", decoded["input"])
	}
	if decoded["output"] != "done" {
		t.Errorf("output = %v, want \"done\"", decoded["output"])
	}
	return decoded
}

// TestSQSExporter_ArrayFormat_ContentAreJSONValues covers every exporter
// that renders through RenderRecord with OperationsFormatArray: the
// operations array carries result as a JSON value and the renamed
// timestamp keys.
func TestSQSExporter_ArrayFormat_ContentAreJSONValues(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.example.com/q"}
	if err := exp.Export(context.Background(), contentTestRecord()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	decoded := assertContentValues(t, []byte(aws.ToString(fake.calls[0].MessageBody)))

	ops, ok := decoded["operations"].([]any)
	if !ok || len(ops) != 2 {
		t.Fatalf("operations = %v, want two entries", decoded["operations"])
	}
	first, ok := ops[0].(map[string]any)
	if !ok {
		t.Fatalf("operations[0] = %v, want an object", ops[0])
	}
	second, ok := ops[1].(map[string]any)
	if !ok {
		t.Fatalf("operations[1] = %v, want an object", ops[1])
	}
	if res, ok := first["result"].(map[string]any); !ok || res["value"] != float64(42) {
		t.Errorf("operations[0].result = %v, want {\"value\": 42}", first["result"])
	}
	if second["result"] != float64(7) {
		t.Errorf("operations[1].result = %v, want 7", second["result"])
	}
	if first["startTime"] != "2026-06-16T17:00:26.500Z" || first["endTime"] != "2026-06-16T17:00:27.500Z" {
		t.Errorf("operation timestamps = %v / %v", first["startTime"], first["endTime"])
	}
	for _, key := range []string{"startTimestamp", "endTimestamp", "input"} {
		if _, present := first[key]; present {
			t.Errorf("operations[0] has key %q: %v", key, first)
		}
	}
}

// TestRenderRecord_ByName_ResultIsJSONValue covers the by-name rendering
// that CloudWatch Logs and DynamoDB use: a single-occurrence summary
// carries result as a JSON value.
func TestRenderRecord_ByName_ResultIsJSONValue(t *testing.T) {
	body, err := RenderRecord(contentTestRecord(), OperationsFormatByName)
	if err != nil {
		t.Fatalf("RenderRecord: %v", err)
	}
	decoded := assertContentValues(t, body)
	byName, ok := decoded["operationsByName"].(map[string]any)
	if !ok {
		t.Fatalf("operationsByName = %v", decoded["operationsByName"])
	}
	inc, ok := byName["increment"].(map[string]any)
	if !ok {
		t.Fatalf("operationsByName.increment = %v, want an object", byName["increment"])
	}
	count, ok := byName["count"].(map[string]any)
	if !ok {
		t.Fatalf("operationsByName.count = %v, want an object", byName["count"])
	}
	if res, ok := inc["result"].(map[string]any); !ok || res["value"] != float64(42) {
		t.Errorf("operationsByName.increment.result = %v, want {\"value\": 42}", inc["result"])
	}
	if count["result"] != float64(7) {
		t.Errorf("operationsByName.count.result = %v, want 7", count["result"])
	}
}

// TestRecord_ContentRoundTrip verifies json.Unmarshal of an emitted record
// restores the content fields as the same JSON values.
func TestRecord_ContentRoundTrip(t *testing.T) {
	rec := contentTestRecord()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var back Record
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(back.Input) != string(rec.Input) || string(back.Output) != string(rec.Output) {
		t.Errorf("input/output = %s / %s, want %s / %s", back.Input, back.Output, rec.Input, rec.Output)
	}
	if len(back.Operations) != 2 || string(back.Operations[0].Result) != string(rec.Operations[0].Result) || string(back.Operations[1].Result) != string(rec.Operations[1].Result) {
		t.Errorf("operation results = %+v", back.Operations)
	}
}
