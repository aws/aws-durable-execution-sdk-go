package insight

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestDecodeRawResult_ValidJSON_Decoded(t *testing.T) {
	got := decodeRawResult(`{"rows":1200}`)
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected a decoded map, got %T: %v", got, got)
	}
	if m["rows"] != float64(1200) {
		t.Errorf("expected rows=1200, got %v", m["rows"])
	}
}

func TestDecodeRawResult_PlainJSONString_Decoded(t *testing.T) {
	got := decodeRawResult(`"hello"`)
	if got != "hello" {
		t.Errorf("expected the decoded string \"hello\", got %v (%T)", got, got)
	}
}

func TestDecodeRawResult_NonJSON_ReturnsRawString(t *testing.T) {
	// A custom Serdes might checkpoint a non-JSON pointer/filepath -
	// matching OperationRecord.Result's own documented caveat.
	got := decodeRawResult("s3://my-bucket/overflow/abc123")
	if got != "s3://my-bucket/overflow/abc123" {
		t.Errorf("expected the raw string unchanged for non-JSON input, got %v", got)
	}
}

func TestDecodeRawResult_Empty_ReturnsNil(t *testing.T) {
	if got := decodeRawResult(""); got != nil {
		t.Errorf("expected nil for an empty raw result, got %v", got)
	}
}

func TestApplyContent_OverrideResult_OptsInAndTransforms(t *testing.T) {
	record := WorkflowInsightRecord{
		Operations: []OperationRecord{
			{Name: "charge-payment", Status: "SUCCEEDED", rawResult: `{"amount":99.99,"internalFee":1.5}`},
			{Name: "other-step", Status: "SUCCEEDED", rawResult: `{"secret":"data"}`},
		},
	}
	applyContent(&record, ContentConfig{
		Operations: OperationsConfig{
			Overrides: []OperationOverride{
				{OperationName: "charge-payment", Result: func(v any) any {
					m := v.(map[string]any)
					return map[string]any{"amount": m["amount"]}
				}},
			},
		},
	})

	var chargeOp, otherOp OperationRecord
	for _, op := range record.Operations {
		if op.Name == "charge-payment" {
			chargeOp = op
		}
		if op.Name == "other-step" {
			otherOp = op
		}
	}

	chargeResult, ok := chargeOp.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected charge-payment's Result to be a redacted map, got %T: %v", chargeOp.Result, chargeOp.Result)
	}
	if chargeResult["amount"] != 99.99 || chargeResult["internalFee"] != nil {
		t.Errorf("expected only amount to survive the transform, got %v", chargeResult)
	}
	if otherOp.Result != nil {
		t.Errorf("expected other-step's Result to stay nil (no matching override), got %v", otherOp.Result)
	}
}

func TestApplyContent_OverrideResult_IncludeAsIs(t *testing.T) {
	record := WorkflowInsightRecord{
		Operations: []OperationRecord{{Name: "s", Status: "SUCCEEDED", rawResult: `{"rows":1200}`}},
	}
	applyContent(&record, ContentConfig{
		Operations: OperationsConfig{
			Overrides: []OperationOverride{{OperationName: "s", Result: IncludeAsIs}},
		},
	})

	result, ok := record.Operations[0].Result.(map[string]any)
	if !ok || result["rows"] != float64(1200) {
		t.Errorf("expected Result to be the decoded value unchanged, got %v", record.Operations[0].Result)
	}
}

func TestApplyContent_NoMatchingOverride_ResultStaysNilByDefault(t *testing.T) {
	record := WorkflowInsightRecord{
		Operations: []OperationRecord{{Name: "s", Status: "SUCCEEDED", rawResult: `{"rows":1200}`}},
	}
	applyContent(&record, ContentConfig{})
	if record.Operations[0].Result != nil {
		t.Errorf("expected Result to stay nil with no matching override (JS SDK's own documented default), got %v", record.Operations[0].Result)
	}
}

func TestOperationsByName_Result_SingleOccurrence(t *testing.T) {
	ops := []OperationRecord{
		{Name: "insert-to-db", Type: "STEP", Status: "SUCCEEDED", Result: map[string]any{"rows": 1200}},
	}
	byName := OperationsByName(ops)
	agg := byName["insert-to-db"]
	m, ok := agg.Result.(map[string]any)
	if !ok || m["rows"] != 1200 {
		t.Errorf("expected Result to be passed through for a single occurrence, got %v", agg.Result)
	}
}

func TestOperationsByName_Result_RepeatedName_StaysNil(t *testing.T) {
	ops := []OperationRecord{
		{Name: "process-item", Type: "STEP", Status: "SUCCEEDED", Result: "first"},
		{Name: "process-item", Type: "STEP", Status: "SUCCEEDED", Result: "second"},
	}
	byName := OperationsByName(ops)
	agg := byName["process-item"]
	if agg.Result != nil {
		t.Errorf("expected Result to stay nil for a repeated name (no single representative value), got %v", agg.Result)
	}
}

func TestTruncate_DropsOperationResultsBeforeWholeOperations(t *testing.T) {
	record := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations: []OperationRecord{
			{ID: "1", Name: "a", Result: map[string]any{"data": "some-large-result-payload-here"}},
			{ID: "2", Name: "b"},
		},
	}
	full := recordSize(t, record)
	// A limit just under the full size - small enough to force SOME
	// truncation, but large enough that dropping just the one Result
	// field (not a whole operation) should be suffient.
	got := Truncate(record, full-5, nil)

	if !got.Truncated {
		t.Fatal("expected Truncated=true")
	}
	// Both operations should still be PRESENT (step 1 alone was
	// sufficient) - only operation "a"'s own Result should be gone.
	if len(got.Operations) != 2 {
		t.Fatalf("expected both operations to survive (only a Result field needed dropping), got %d", len(got.Operations))
	}
	for _, op := range got.Operations {
		if op.Name == "a" {
			if op.Result != nil {
				t.Error("expected operation 'a's Result to have been dropped")
			}
			if !op.Truncated {
				t.Error("expected operation 'a' to be marked Truncated")
			}
		}
	}
	if got.DroppedOperations != 0 {
		t.Errorf("expected DroppedOperations=0 (step 1 alone was enough, no whole operation needed dropping), got %d", got.DroppedOperations)
	}
}

func TestTruncate_NoOperationHasResult_SkipsStep1Cleanly(t *testing.T) {
	// Confirms step 1 is a true no-op (not an error, not a panic) when
	// no operation has a Result set at all - the common case, matching
	// the JS SDK's own documented "results not included by default."
	record := WorkflowInsightRecord{
		ExecutionARN: "arn:test",
		Operations:   []OperationRecord{{ID: "1", Name: "a"}, {ID: "2", Name: "b"}},
	}
	got := Truncate(record, 1_000_000, nil)
	if got.Truncated {
		t.Error("expected no truncation needed at all for such a small record")
	}
}

func TestPlugin_OperationOverrideResult_EndToEnd(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{
		Exporters: []Exporter{exp},
		Content: ContentConfig{
			Operations: OperationsConfig{
				Overrides: []OperationOverride{
					{OperationName: "compute", Result: IncludeAsIs},
				},
			},
		},
	})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.Step(dc, "compute", func(sc types.StepContext) (map[string]any, error) {
			return map[string]any{"total": 42}, nil
		})
		if err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-result", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	var computeOp *OperationRecord
	for i, op := range records[0].Operations {
		if op.Name == "compute" {
			computeOp = &records[0].Operations[i]
		}
	}
	if computeOp == nil {
		t.Fatal("expected to find the 'compute' operation")
	}
	m, ok := computeOp.Result.(map[string]any)
	if !ok || m["total"] != float64(42) {
		t.Errorf("expected Result={total:42} (opted in via override), got %v", computeOp.Result)
	}
}

func TestPlugin_NoOverride_ResultExcludedByDefault_EndToEnd(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{exp}}) // no Content config at all

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.Step(dc, "compute", func(sc types.StepContext) (map[string]any, error) {
			return map[string]any{"total": 42}, nil
		})
		if err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-no-result", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	for _, op := range records[0].Operations {
		if op.Result != nil {
			t.Errorf("expected NO operation's Result to be included by default (JS SDK's own documented default), got op %+v", op)
		}
	}
}

func TestOperationRecord_ResultOmittedFromJSONWhenNil(t *testing.T) {
	rec := OperationRecord{ID: "1", Name: "a", Type: "STEP", Status: "SUCCEEDED"}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := fields["result"]; present {
		t.Error("expected 'result' key to be omitted when Result is nil")
	}
}

func TestOperationRecord_RawResultNeverMarshaled(t *testing.T) {
	rec := OperationRecord{ID: "1", Name: "a", rawResult: `{"secret":"internal"}`}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) == "" {
		t.Fatal("expected non-empty JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := fields["rawResult"]; present {
		t.Error("expected the unexported rawResult field to never appear in JSON output at all")
	}
}
