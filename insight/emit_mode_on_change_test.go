package insight

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

func TestPlugin_EmitModeOnChange_EmitsMultipleRunningSnapshotsPlusFinal(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{EmitMode: EmitModeOnChange, Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		if _, err := operations.Step(dc, "step-1", func(sc types.StepContext) (string, error) {
			return "ok", nil
		}); err != nil {
			return testResult{}, err
		}
		if _, err := operations.Step(dc, "step-2", func(sc types.StepContext) (string, error) {
			return "ok", nil
		}); err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-onchange", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got %s", out.Status)
	}

	records := exp.snapshot()
	if len(records) < 2 {
		t.Fatalf("expected at least 2 emitted records (some RUNNING snapshots + 1 final), got %d", len(records))
	}

	last := records[len(records)-1]
	if last.Status != ExecutionStatusSucceeded {
		t.Errorf("expected the LAST emitted record to be the final SUCCEEDED one, got status=%s", last.Status)
	}

	sawRunning := false
	for _, rec := range records[:len(records)-1] {
		if rec.Status == ExecutionStatusRunning {
			sawRunning = true
		}
	}
	if !sawRunning {
		t.Error("expected at least one RUNNING snapshot to have been emitted before the final record")
	}
}

func TestPlugin_EmitModeOnComplete_NeverEmitsRunningSnapshots(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{exp}}) // default: EmitModeOnComplete

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		if _, err := operations.Step(dc, "step-1", func(sc types.StepContext) (string, error) {
			return "ok", nil
		}); err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-oncomplete-nochange", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record under EmitModeOnComplete (no mid-flight RUNNING snapshots), got %d", len(records))
	}
	if records[0].Status != ExecutionStatusSucceeded {
		t.Errorf("expected the single record to be SUCCEEDED, got %s", records[0].Status)
	}
}

func TestPlugin_EmitModeOnChange_SampledOut_EmitsNothing(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:1"
	rate := 0.0000001
	if sampledIn(arn, rate) {
		t.Skip("this specific ARN happens to sample IN at this rate - not a real failure, just an unlucky test-input choice")
	}
	insightPlugin := New(Config{EmitMode: EmitModeOnChange, SamplingRate: rate, Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		if _, err := operations.Step(dc, "step-1", func(sc types.StepContext) (string, error) {
			return "ok", nil
		}); err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{Client: client, Plugins: []plugin.InstrumentationPlugin{insightPlugin}})
	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-onchange-sampled-out", arn, eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if records := exp.snapshot(); len(records) != 0 {
		t.Fatalf("expected 0 emitted records (mid-flight AND final) for a sampled-out execution under EmitModeOnChange, got %d", len(records))
	}
}

func TestNew_UnrecognizedEmitMode_DefaultsToOnComplete(t *testing.T) {
	p := New(Config{EmitMode: "not-a-real-mode"})
	if p.emitMode != EmitModeOnComplete {
		t.Errorf("expected an unrecognized EmitMode to default to EmitModeOnComplete, got %v", p.emitMode)
	}
}

func TestNew_EmitModeOnChange_Accepted(t *testing.T) {
	p := New(Config{EmitMode: EmitModeOnChange})
	if p.emitMode != EmitModeOnChange {
		t.Errorf("expected EmitModeOnChange to be accepted as-is, got %v", p.emitMode)
	}
}
