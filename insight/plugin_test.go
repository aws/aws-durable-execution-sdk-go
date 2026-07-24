package insight

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// capturingExporter records every record it receives, guarded by a mutex
// since dispatch calls Export concurrently.
type capturingExporter struct {
	mu      sync.Mutex
	records []Record
	flushed int
}

func (e *capturingExporter) Export(_ context.Context, record Record) error {
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

func (e *capturingExporter) snapshot() []Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Record{}, e.records...)
}

// panickyExporter panics on Export to test panic recovery.
type panickyExporter struct{}

func (panickyExporter) Export(context.Context, Record) error { panic("boom") }

func TestPlugin_HappyPath_EmitsSucceededRecord(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}})

	ctx := context.Background()
	now := time.Now()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:order-processor:qual/durable-execution/exec-1/inv-1"

	// Simulate invocation lifecycle.
	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: now,
		ExecutionInput:          map[string]string{"orderId": "abc"},
	})

	ip.onOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "validate",
		Type:           "STEP",
		SubType:        "Step",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now.Add(10 * time.Millisecond),
		ParentID:       "",
	})

	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "validate",
		Type:           "STEP",
		SubType:        "Step",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(10 * time.Millisecond),
		EndTimestamp:   now.Add(50 * time.Millisecond),
		Result:         `"validated:abc"`,
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: map[string]string{"status": "done"},
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	rec := records[0]

	if rec.RecordType != RecordType {
		t.Errorf("expected RecordType=%q, got %q", RecordType, rec.RecordType)
	}
	if rec.Status != StatusSucceeded {
		t.Errorf("expected Status=SUCCEEDED, got %s", rec.Status)
	}
	if rec.FunctionName != "order-processor" {
		t.Errorf("expected FunctionName=order-processor, got %q", rec.FunctionName)
	}
	if rec.ExecutionName != "exec-1" {
		t.Errorf("expected ExecutionName=exec-1, got %q", rec.ExecutionName)
	}
	if rec.Output == nil {
		t.Error("expected non-nil Output on a succeeded record")
	}
	if rec.EndTimestamp == nil || rec.DurationMs == nil {
		t.Error("expected EndTimestamp and DurationMs on a terminal record")
	}
	if rec.EmittedAt.IsZero() {
		t.Error("expected EmittedAt to be set")
	}
	if len(rec.Operations) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(rec.Operations))
	}
	op := rec.Operations[0]
	if op.Name != "validate" || op.Type != "STEP" || op.Status != "SUCCEEDED" {
		t.Errorf("expected {validate STEP SUCCEEDED}, got %+v", op)
	}
	if exp.flushed != 1 {
		t.Errorf("expected Flush called once, got %d", exp.flushed)
	}
}

func TestPlugin_FailedExecution_EmitsFailedRecordWithError(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}})

	ctx := context.Background()
	now := time.Now()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: now,
	})

	ip.onOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "failing-step",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now,
	})

	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "failing-step",
		Type:           "STEP",
		Status:         durable.PluginOperationFailed,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
		Error:          &testError{msg: "boom"},
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:   arn,
		Status:         durable.PluginInvocationFailed,
		ExecutionError: &testError{msg: "boom"},
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	rec := records[0]
	if rec.Status != StatusFailed {
		t.Errorf("expected Status=FAILED, got %s", rec.Status)
	}
	if rec.Error == nil {
		t.Fatal("expected non-nil Error on failed record")
	}
	if rec.Error.Message != "boom" {
		t.Errorf("expected error message %q, got %q", "boom", rec.Error.Message)
	}
	if len(rec.Operations) != 1 || rec.Operations[0].Status != "FAILED" {
		t.Errorf("expected 1 FAILED operation, got %+v", rec.Operations)
	}
}

func TestPlugin_PendingInvocation_DoesNotEmit(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn: arn,
		Status:       durable.PluginInvocationPending,
	})

	if records := exp.snapshot(); len(records) != 0 {
		t.Fatalf("expected 0 records for pending invocation, got %d", len(records))
	}
}

func TestPlugin_MultipleExporters_AllReceiveRecord(t *testing.T) {
	exp1 := &capturingExporter{}
	exp2 := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp1, exp2}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	if len(exp1.snapshot()) != 1 {
		t.Errorf("exporter 1: expected 1 record, got %d", len(exp1.snapshot()))
	}
	if len(exp2.snapshot()) != 1 {
		t.Errorf("exporter 2: expected 1 record, got %d", len(exp2.snapshot()))
	}
}

func TestPlugin_ExporterPanic_DoesNotAffectOtherExporters(t *testing.T) {
	good := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{panickyExporter{}, good}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	if len(good.snapshot()) != 1 {
		t.Errorf("expected well-behaved exporter to receive 1 record, got %d", len(good.snapshot()))
	}
}

func TestPlugin_DefaultConfig_UsesLambdaLogExporter(t *testing.T) {
	ip := New(Config{})
	if len(ip.exporters) != 1 {
		t.Fatalf("expected 1 default exporter, got %d", len(ip.exporters))
	}
	if _, ok := ip.exporters[0].(*LambdaLogExporter); !ok {
		t.Errorf("expected default exporter to be *LambdaLogExporter, got %T", ip.exporters[0])
	}
	if ip.emitMode != EmitOnComplete {
		t.Errorf("expected default EmitMode=on-complete, got %v", ip.emitMode)
	}
}

func TestPlugin_OperationDetailTopLevel_FiltersChildOps(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}}) // default: TopLevel

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"
	now := time.Now()

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	// Top-level op.
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "parent-op",
		Name:           "parent",
		Type:           "CONTEXT",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	// Child op (should be filtered out).
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "child-op",
		Name:           "child",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		ParentID:       "parent-op",
		StartTimestamp: now,
		EndTimestamp:   now.Add(5 * time.Millisecond),
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	// Only the top-level op should remain.
	if len(records[0].Operations) != 1 {
		t.Fatalf("expected 1 top-level op, got %d", len(records[0].Operations))
	}
	if records[0].Operations[0].Name != "parent" {
		t.Errorf("expected parent op, got %q", records[0].Operations[0].Name)
	}
}

func TestPlugin_OperationDetailFullTree_IncludesChildOps(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{
		OperationDetail: OperationDetailFullTree,
		Exporters:       []Exporter{exp},
	})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"
	now := time.Now()

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "parent-op",
		Name:           "parent",
		Type:           "CONTEXT",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "child-op",
		Name:           "child",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		ParentID:       "parent-op",
		StartTimestamp: now,
		EndTimestamp:   now.Add(5 * time.Millisecond),
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if len(records[0].Operations) != 2 {
		t.Fatalf("expected 2 ops (full tree), got %d", len(records[0].Operations))
	}
}

func TestPlugin_SkipsExecutionOperation(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})

	// The EXECUTION operation should be skipped.
	ip.onOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn: arn,
		ID:           "exec-root",
		Type:         "EXECUTION",
		Status:       durable.PluginOperationStarted,
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if len(records[0].Operations) != 0 {
		t.Errorf("expected 0 operations (EXECUTION excluded), got %d", len(records[0].Operations))
	}
}

func TestPlugin_EnrichLogContext(t *testing.T) {
	ip := New(Config{})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	// Before invocation starts, should return nil.
	if m := ip.enrichLogContext(ctx); m != nil {
		t.Errorf("expected nil before invocation, got %v", m)
	}

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})

	m := ip.enrichLogContext(ctx)
	if m == nil {
		t.Fatal("expected non-nil log context after invocation start")
	}
	if m["execution_arn"] != arn {
		t.Errorf("expected execution_arn=%q, got %v", arn, m["execution_arn"])
	}
	if m["function_name"] != "fn" {
		t.Errorf("expected function_name=%q, got %v", "fn", m["function_name"])
	}
}

func TestPlugin_OnOperationChange_BackfillsOps(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"
	now := time.Now()

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	// Simulate operations that changed externally between invocations.
	ip.onOperationChange(ctx, durable.OperationChangeHookInfo{
		ExecutionArn: arn,
		UpdatedOperations: map[string]durable.OperationHookInfo{
			"op-prev": {
				ExecutionArn:   arn,
				ID:             "op-prev",
				Name:           "previous-step",
				Type:           "STEP",
				Status:         durable.PluginOperationSucceeded,
				StartTimestamp: now.Add(-100 * time.Millisecond),
				EndTimestamp:   now.Add(-50 * time.Millisecond),
			},
		},
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "done",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if len(records[0].Operations) != 1 {
		t.Fatalf("expected 1 op from OnOperationChange backfill, got %d", len(records[0].Operations))
	}
	if records[0].Operations[0].Name != "previous-step" {
		t.Errorf("expected backfilled op name=previous-step, got %q", records[0].Operations[0].Name)
	}
}

func TestPlugin_DurablePlugin_WiresHooksCorrectly(t *testing.T) {
	ip := New(Config{})
	dp := ip.Plugin()

	if dp.OnInvocationStart == nil {
		t.Error("expected OnInvocationStart to be set")
	}
	if dp.OnInvocationEnd == nil {
		t.Error("expected OnInvocationEnd to be set")
	}
	if dp.OnOperationStart == nil {
		t.Error("expected OnOperationStart to be set")
	}
	if dp.OnOperationEnd == nil {
		t.Error("expected OnOperationEnd to be set")
	}
	if dp.OnOperationChange == nil {
		t.Error("expected OnOperationChange to be set")
	}
	if dp.EnrichLogContext == nil {
		t.Error("expected EnrichLogContext to be set")
	}
}

// testError is a simple error type for testing.
type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }
