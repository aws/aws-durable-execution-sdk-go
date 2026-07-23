package insight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

func TestApplyValueTransform_NilTransform_ReturnsValueUnchanged(t *testing.T) {
	got := applyValueTransform(nil, map[string]any{"a": 1})
	m, ok := got.(map[string]any)
	if !ok || m["a"] != 1 {
		t.Errorf("expected the value unchanged, got %v", got)
	}
}

func TestApplyValueTransform_ExcludeContent_ReturnsNil(t *testing.T) {
	got := applyValueTransform(ExcludeContent, map[string]any{"a": 1})
	if got != nil {
		t.Errorf("expected ExcludeContent to return nil, got %v", got)
	}
}

func TestApplyValueTransform_IncludeAsIs_ReturnsValueUnchanged(t *testing.T) {
	got := applyValueTransform(IncludeAsIs, "hello")
	if got != "hello" {
		t.Errorf("expected IncludeAsIs to return the value unchanged, got %v", got)
	}
}

func TestApplyValueTransform_CustomTransform_Applied(t *testing.T) {
	redact := func(v any) any { return map[string]any{"redacted": true} }
	got := applyValueTransform(redact, map[string]any{"secret": "value"})
	m, ok := got.(map[string]any)
	if !ok || m["redacted"] != true {
		t.Errorf("expected the custom transform's own return value, got %v", got)
	}
}

func TestOperationsConfig_ShouldIncludeErrors_DefaultsToTrue(t *testing.T) {
	var cfg OperationsConfig // IncludeErrors left nil
	if !cfg.ShouldIncludeErrors() {
		t.Error("expected ShouldIncludeErrors to default to true when IncludeErrors is nil")
	}
}

func TestOperationsConfig_ShouldIncludeErrors_ExplicitFalse(t *testing.T) {
	f := false
	cfg := OperationsConfig{IncludeErrors: &f}
	if cfg.ShouldIncludeErrors() {
		t.Error("expected ShouldIncludeErrors to be false when explicitly set")
	}
}

func TestApplyOverride_LastMatchingEntryWins(t *testing.T) {
	overrides := []OperationOverride{
		{OperationName: "step-a", Exclude: false},
		{OperationName: "step-a", Exclude: true}, // later entry, same name - should win
	}
	ov, found := applyOverride(overrides, "step-a")
	if !found {
		t.Fatal("expected a match for step-a")
	}
	if !ov.Exclude {
		t.Error("expected the LAST matching entry's own Exclude=true to win, got false")
	}
}

func TestApplyOverride_NoMatch_ReturnsFoundFalse(t *testing.T) {
	_, found := applyOverride([]OperationOverride{{OperationName: "other"}}, "step-a")
	if found {
		t.Error("expected found=false when no override matches")
	}
}

func TestApplyContent_InputOutputTransforms(t *testing.T) {
	record := WorkflowInsightRecord{
		Input:  map[string]any{"orderId": "12345", "creditCard": "4111-1111-1111-1111"},
		Output: map[string]any{"status": "completed", "internalTraceId": "abc"},
	}
	cfg := ContentConfig{
		Input:  func(v any) any { m := v.(map[string]any); return map[string]any{"orderId": m["orderId"]} },
		Output: ExcludeContent,
	}
	applyContent(&record, cfg)

	inputMap, ok := record.Input.(map[string]any)
	if !ok || inputMap["orderId"] != "12345" || inputMap["creditCard"] != nil {
		t.Errorf("expected Input to be redacted to only orderId, got %v", record.Input)
	}
	if record.Output != nil {
		t.Errorf("expected Output to be excluded (nil), got %v", record.Output)
	}
}

func TestApplyContent_IncludeErrorsFalse_StripsAllOperationErrors(t *testing.T) {
	f := false
	record := WorkflowInsightRecord{
		Operations: []OperationRecord{
			{Name: "a", Status: "FAILED", Error: &ErrorDetail{Name: "Err", Message: "boom"}},
			{Name: "b", Status: "SUCCEEDED"},
		},
	}
	applyContent(&record, ContentConfig{Operations: OperationsConfig{IncludeErrors: &f}})

	for _, op := range record.Operations {
		if op.Error != nil {
			t.Errorf("expected every operation's Error to be stripped, got %+v", op)
		}
	}
}

func TestApplyContent_OverrideExclude_RemovesOperation(t *testing.T) {
	record := WorkflowInsightRecord{
		Operations: []OperationRecord{
			{Name: "keep-me", Status: "SUCCEEDED"},
			{Name: "internal-log", Status: "SUCCEEDED"},
		},
	}
	applyContent(&record, ContentConfig{Operations: OperationsConfig{
		Overrides: []OperationOverride{{OperationName: "internal-log", Exclude: true}},
	}})

	if len(record.Operations) != 1 || record.Operations[0].Name != "keep-me" {
		t.Errorf("expected only 'keep-me' to remain, got %+v", record.Operations)
	}
}

func TestApplyContent_NoOverrides_LeavesOperationsUnchanged(t *testing.T) {
	record := WorkflowInsightRecord{
		Operations: []OperationRecord{{Name: "a"}, {Name: "b"}},
	}
	applyContent(&record, ContentConfig{})
	if len(record.Operations) != 2 {
		t.Errorf("expected no operations dropped with no overrides configured, got %d", len(record.Operations))
	}
}

func TestApplyContent_ZeroValueConfig_LeavesRecordUnchanged(t *testing.T) {
	record := WorkflowInsightRecord{
		Input:      "raw-input",
		Output:     "raw-output",
		Operations: []OperationRecord{{Name: "a", Error: &ErrorDetail{Name: "E", Message: "m"}}},
	}
	original := record
	applyContent(&record, ContentConfig{})

	if record.Input != original.Input || record.Output != original.Output {
		t.Error("expected the zero-value ContentConfig to leave Input/Output unchanged")
	}
	if len(record.Operations) != 1 || record.Operations[0].Error == nil {
		t.Error("expected the zero-value ContentConfig to leave operation errors included")
	}
}

func TestPlugin_ContentConfig_EndToEnd_RedactsInputAndExcludesOperation(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{
		Exporters: []Exporter{exp},
		Content: ContentConfig{
			Input: func(v any) any {
				return map[string]any{"redacted": true}
			},
			Operations: OperationsConfig{
				Overrides: []OperationOverride{{OperationName: "sensitive-step", Exclude: true}},
			},
		},
	})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.Step(dc, "sensitive-step", func(sc types.StepContext) (string, error) {
			return "ok", nil
		})
		if err != nil {
			return testResult{}, err
		}
		_, err = operations.Step(dc, "visible-step", func(sc types.StepContext) (string, error) {
			return "ok", nil
		})
		return testResult{Status: "done"}, err
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "sensitive-order-id"})
	input := newInvocationInput("exec-content", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	rec := records[0]

	inputMap, ok := rec.Input.(map[string]any)
	if !ok || inputMap["redacted"] != true {
		t.Errorf("expected Input to be redacted, got %v", rec.Input)
	}
	for _, op := range rec.Operations {
		if op.Name == "sensitive-step" {
			t.Errorf("expected 'sensitive-step' to be excluded, but found it: %+v", op)
		}
	}
	foundVisible := false
	for _, op := range rec.Operations {
		if op.Name == "visible-step" {
			foundVisible = true
		}
	}
	if !foundVisible {
		t.Error("expected 'visible-step' to still be present")
	}
}

func TestPlugin_ContentConfig_IncludeErrorsFalse_EndToEnd(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	f := false
	insightPlugin := New(Config{
		Exporters: []Exporter{exp},
		Content:   ContentConfig{Operations: OperationsConfig{IncludeErrors: &f}},
	})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.Step(dc, "always-fails", func(sc types.StepContext) (string, error) {
			return "", errors.New("boom")
		}, operations.WithStepRetryStrategy[string](func(err error, attempt int) types.RetryDecision {
			return types.RetryDecision{ShouldRetry: false}
		}))
		return testResult{}, err
	}
	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-content-errors", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	for _, op := range records[0].Operations {
		if op.Error != nil {
			t.Errorf("expected no operation errors under IncludeErrors=false, got %+v", op)
		}
	}
}
