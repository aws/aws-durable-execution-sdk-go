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

func TestFilterTopLevel_DropsOperationsWithParentID(t *testing.T) {
	ops := []OperationRecord{
		{ID: "1", Name: "top-1", Type: "STEP"},
		{ID: "2", Name: "child-1", Type: "STEP", ParentID: "ctx-1"},
		{ID: "3", Name: "top-2", Type: "CONTEXT"},
		{ID: "4", Name: "child-2", Type: "STEP", ParentID: "ctx-1"},
	}
	filtered := filterTopLevel(ops)

	if len(filtered) != 2 {
		t.Fatalf("expected 2 top-level operations, got %d: %+v", len(filtered), filtered)
	}
	for _, op := range filtered {
		if op.ParentID != "" {
			t.Errorf("expected only ParentID=='' operations, got %+v", op)
		}
	}
}

func TestFilterTopLevel_DoesNotMutateInput(t *testing.T) {
	original := []OperationRecord{
		{ID: "1", Name: "top", Type: "STEP"},
		{ID: "2", Name: "child", Type: "STEP", ParentID: "ctx-1"},
	}
	originalLen := len(original)

	_ = filterTopLevel(original)

	if len(original) != originalLen {
		t.Errorf("expected filterTopLevel to never mutate its input slice, original length changed from %d to %d", originalLen, len(original))
	}
}

func TestFilterTopLevel_EmptyInput(t *testing.T) {
	filtered := filterTopLevel(nil)
	if len(filtered) != 0 {
		t.Errorf("expected an empty result for nil input, got %d", len(filtered))
	}
}

func TestPlugin_OperationDetailTopLevel_ExcludesChildOperations(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{exp}}) // OperationDetail left unset (default: top-level)

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.RunInChildContext(dc, "child-ctx", func(child types.DurableContext) (string, error) {
			return operations.Step(child, "nested-step", func(sc types.StepContext) (string, error) {
				return "ok", nil
			})
		})
		if err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-detail-top", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	rec := records[0]
	// Only the top-level "child-ctx" CONTEXT operation should be
	// present; "nested-step" (ParentID == child-ctx's own operation ID)
	// must be excluded under the default OperationDetailTopLevel.
	for _, op := range rec.Operations {
		if op.ParentID != "" {
			t.Errorf("expected no operation with a non-empty ParentID under OperationDetailTopLevel, got %+v", op)
		}
	}
	foundTopLevelContext := false
	for _, op := range rec.Operations {
		if op.Name == "child-ctx" {
			foundTopLevelContext = true
		}
		if op.Name == "nested-step" {
			t.Errorf("expected 'nested-step' (a child operation) to be excluded, but found it: %+v", op)
		}
	}
	if !foundTopLevelContext {
		t.Error("expected the top-level 'child-ctx' operation to still be present")
	}
}

func TestPlugin_OperationDetailFullTree_IncludesChildOperations(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{OperationDetail: OperationDetailFullTree, Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.RunInChildContext(dc, "child-ctx", func(child types.DurableContext) (string, error) {
			return operations.Step(child, "nested-step", func(sc types.StepContext) (string, error) {
				return "ok", nil
			})
		})
		if err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-detail-full", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	rec := records[0]

	foundNestedStep := false
	for _, op := range rec.Operations {
		if op.Name == "nested-step" {
			foundNestedStep = true
			if op.ParentID == "" {
				t.Error("expected 'nested-step' to have a non-empty ParentID (it IS a child operation)")
			}
		}
	}
	if !foundNestedStep {
		t.Errorf("expected 'nested-step' (a child operation) to be INCLUDED under OperationDetailFullTree, got operations: %+v", rec.Operations)
	}
}

func TestNew_DefaultsOperationDetailToTopLevel(t *testing.T) {
	p := New(Config{})
	if p.operationDetail != OperationDetailTopLevel {
		t.Errorf("expected the default OperationDetail to be OperationDetailTopLevel, got %v", p.operationDetail)
	}
}

func TestNew_UnrecognizedOperationDetail_DefaultsToTopLevel(t *testing.T) {
	p := New(Config{OperationDetail: "not-a-real-value"})
	if p.operationDetail != OperationDetailTopLevel {
		t.Errorf("expected an unrecognized OperationDetail value to default to OperationDetailTopLevel, got %v", p.operationDetail)
	}
}
