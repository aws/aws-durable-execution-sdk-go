package insight

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

type testEvent struct {
	OrderID string `json:"orderId"`
}

type testResult struct {
	Status string `json:"status"`
}

// capturingExporter records every record it receives, guarded by a
// mutex since Plugin.dispatch calls every configured Exporter's own
// Export concurrently.
type capturingExporter struct {
	mu      sync.Mutex
	records []WorkflowInsightRecord
	flushed int
}

func (e *capturingExporter) Export(_ context.Context, record WorkflowInsightRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, record)
	return nil
}

func (e *capturingExporter) Flush(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flushed++
	return nil
}

func (e *capturingExporter) snapshot() []WorkflowInsightRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]WorkflowInsightRecord{}, e.records...)
}

func newInvocationInput(executionID, arn string, eventPayload []byte) types.DurableExecutionInvocationInput {
	payload := string(eventPayload)
	return types.DurableExecutionInvocationInput{
		DurableExecutionArn: arn,
		CheckpointToken:     "token-0",
		InitialExecutionState: types.InitialExecutionState{
			Operations: []types.Operation{{
				ID:               executionID,
				Type:             types.OperationTypeExecution,
				Status:           types.OperationStatusStarted,
				ExecutionDetails: &types.ExecutionDetails{InputPayload: &payload},
			}},
		},
	}
}

func TestPlugin_HappyPath_EmitsSucceededRecordWithOperations(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.Step(dc, "validate", func(sc types.StepContext) (string, error) {
			return "validated:" + event.OrderID, nil
		})
		if err != nil {
			return testResult{}, err
		}
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  client,
		Plugins: []plugin.InstrumentationPlugin{insightPlugin},
	})

	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-1", "arn:aws:lambda:us-east-1:123456789012:function:order-processor:1", eventPayload)

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got %s", out.Status)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record (EmitModeOnComplete, one terminal outcome), got %d", len(records))
	}
	rec := records[0]

	if rec.RecordType != RecordType {
		t.Errorf("expected RecordType=%q, got %q", RecordType, rec.RecordType)
	}
	if rec.Status != ExecutionStatusSucceeded {
		t.Errorf("expected Status=Succeeded, got %s", rec.Status)
	}
	if rec.FunctionName != "order-processor" {
		t.Errorf("expected FunctionName=order-processor, got %q", rec.FunctionName)
	}
	if rec.Region != "us-east-1" || rec.AccountID != "123456789012" {
		t.Errorf("expected Region=us-east-1 AccountID=123456789012, got Region=%q AccountID=%q", rec.Region, rec.AccountID)
	}
	if rec.Output == nil {
		t.Error("expected a non-nil Output on a succeeded record")
	}
	if rec.EndTime == nil || rec.DurationMs == nil {
		t.Error("expected EndTime and DurationMs to be set on a terminal record")
	}
	if len(rec.Operations) != 1 {
		t.Fatalf("expected exactly 1 operation record (the Step; the root EXECUTION operation is excluded), got %d: %+v", len(rec.Operations), rec.Operations)
	}
	op := rec.Operations[0]
	if op.Name != "validate" || op.Type != "STEP" || op.Status != "SUCCEEDED" {
		t.Errorf("expected operation {name:validate type:STEP status:SUCCEEDED}, got %+v", op)
	}

	if exp.flushed != 1 {
		t.Errorf("expected the exporter's Flush to be called exactly once, got %d", exp.flushed)
	}
}

func TestPlugin_FailedExecution_EmitsFailedRecordWithError(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		_, err := operations.Step(dc, "always-fails", func(sc types.StepContext) (string, error) {
			return "", errors.New("boom")
		}, operations.WithStepRetryStrategy[string](func(err error, attempt int) types.RetryDecision {
			return types.RetryDecision{ShouldRetry: false}
		}))
		return testResult{}, err
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  client,
		Plugins: []plugin.InstrumentationPlugin{insightPlugin},
	})

	eventPayload, _ := json.Marshal(testEvent{OrderID: "fail-me"})
	input := newInvocationInput("exec-2", "arn:aws:lambda:us-east-1:123456789012:function:order-processor:1", eventPayload)

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusFailed {
		t.Fatalf("expected Failed, got %s", out.Status)
	}

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 emitted record, got %d", len(records))
	}
	rec := records[0]
	if rec.Status != ExecutionStatusFailed {
		t.Errorf("expected Status=Failed, got %s", rec.Status)
	}
	if rec.Error == nil {
		t.Fatal("expected a non-nil Error on a failed record")
	}
	if rec.Error.Message != "boom" {
		t.Errorf("expected error message %q, got %q", "boom", rec.Error.Message)
	}
	if len(rec.Operations) != 1 || rec.Operations[0].Status != "FAILED" {
		t.Errorf("expected exactly 1 FAILED operation record, got %+v", rec.Operations)
	}
}

func TestPlugin_EmitModeOnFailure_SkipsSuccessfulExecutions(t *testing.T) {
	client := newFakeClient()
	exp := &capturingExporter{}
	insightPlugin := New(Config{EmitMode: EmitModeOnFailure, Exporters: []Exporter{exp}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  client,
		Plugins: []plugin.InstrumentationPlugin{insightPlugin},
	})

	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-3", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded, got %s", out.Status)
	}

	if records := exp.snapshot(); len(records) != 0 {
		t.Fatalf("expected 0 emitted records under EmitModeOnFailure for a successful execution, got %d", len(records))
	}
}

func TestPlugin_MultipleExporters_AllReceiveTheRecord(t *testing.T) {
	client := newFakeClient()
	exp1 := &capturingExporter{}
	exp2 := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{exp1, exp2}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  client,
		Plugins: []plugin.InstrumentationPlugin{insightPlugin},
	})

	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-4", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	if _, err := entry(context.Background(), input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(exp1.snapshot()) != 1 {
		t.Errorf("expected exporter 1 to receive exactly 1 record, got %d", len(exp1.snapshot()))
	}
	if len(exp2.snapshot()) != 1 {
		t.Errorf("expected exporter 2 to receive exactly 1 record, got %d", len(exp2.snapshot()))
	}
}

func TestPlugin_DefaultConfig_UsesLambdaLogExporter(t *testing.T) {
	p := New(Config{})
	if len(p.exporters) != 1 {
		t.Fatalf("expected exactly 1 default exporter, got %d", len(p.exporters))
	}
	if _, ok := p.exporters[0].(*LambdaLogExporter); !ok {
		t.Errorf("expected the default exporter to be a *LambdaLogExporter, got %T", p.exporters[0])
	}
	if p.emitMode != EmitModeOnComplete {
		t.Errorf("expected the default EmitMode to be EmitModeOnComplete, got %v", p.emitMode)
	}
}

func TestPlugin_ExporterPanic_DoesNotAffectOtherExportersOrExecution(t *testing.T) {
	client := newFakeClient()
	panicky := &panickyExporter{}
	good := &capturingExporter{}
	insightPlugin := New(Config{Exporters: []Exporter{panicky, good}})

	handler := func(event testEvent, dc types.DurableContext) (testResult, error) {
		return testResult{Status: "done"}, nil
	}

	entry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  client,
		Plugins: []plugin.InstrumentationPlugin{insightPlugin},
	})

	eventPayload, _ := json.Marshal(testEvent{OrderID: "abc"})
	input := newInvocationInput("exec-5", "arn:aws:lambda:us-east-1:123456789012:function:f:1", eventPayload)

	out, err := entry(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("expected Succeeded despite one exporter panicking, got %s", out.Status)
	}
	if len(good.snapshot()) != 1 {
		t.Errorf("expected the well-behaved exporter to still receive its record, got %d", len(good.snapshot()))
	}
}

type panickyExporter struct{}

func (panickyExporter) Export(context.Context, WorkflowInsightRecord) error { panic("boom") }
