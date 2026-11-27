package telemetry

import (
	"context"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// capturingExporter records every record it receives, guarded by a mutex
// since dispatch calls Export concurrently.
type capturingExporter struct {
	mu       sync.Mutex
	records  []Record
	flushed  int
	exported chan struct{}
}

func (e *capturingExporter) Export(_ context.Context, record Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, record)
	if e.exported != nil {
		e.exported <- struct{}{}
	}
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
	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: now,
		ExecutionInput:          map[string]string{"orderId": "abc"},
	})

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: map[string]string{"status": "done"},
		Operations: opsByID(durable.OperationHookInfo{
			ExecutionArn:   arn,
			ID:             "op-1",
			Name:           "validate",
			Type:           "STEP",
			SubType:        "Step",
			Status:         durable.PluginOperationSucceeded,
			StartTimestamp: now.Add(10 * time.Millisecond),
			EndTimestamp:   now.Add(50 * time.Millisecond),
			Result:         `"validated:abc"`,
		}),
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: now,
	})

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:   arn,
		Status:         durable.PluginInvocationFailed,
		ExecutionError: &testError{msg: "boom"},
		Operations: opsByID(durable.OperationHookInfo{
			ExecutionArn:   arn,
			ID:             "op-1",
			Name:           "failing-step",
			Type:           "STEP",
			Status:         durable.PluginOperationFailed,
			StartTimestamp: now,
			EndTimestamp:   now.Add(10 * time.Millisecond),
			Error:          checkpointedError("failing-step", "Error", "boom"),
		}),
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
	// The execution error is built from the Go error the handler returned:
	// its type name and its text.
	if rec.Error.Type != "testError" || rec.Error.Message != "boom" {
		t.Errorf("execution error = %+v, want type testError and message boom", *rec.Error)
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
		Operations: opsByID(
			// Top-level op.
			durable.OperationHookInfo{
				ExecutionArn:   arn,
				ID:             "parent-op",
				Name:           "parent",
				Type:           "CONTEXT",
				Status:         durable.PluginOperationSucceeded,
				StartTimestamp: now,
				EndTimestamp:   now.Add(10 * time.Millisecond),
			},
			// Child op (should be filtered out).
			durable.OperationHookInfo{
				ExecutionArn:   arn,
				ID:             "child-op",
				Name:           "child",
				Type:           "STEP",
				Status:         durable.PluginOperationSucceeded,
				ParentID:       "parent-op",
				StartTimestamp: now,
				EndTimestamp:   now.Add(5 * time.Millisecond),
			},
		),
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
		Operations: opsByID(
			durable.OperationHookInfo{
				ExecutionArn:   arn,
				ID:             "parent-op",
				Name:           "parent",
				Type:           "CONTEXT",
				Status:         durable.PluginOperationSucceeded,
				StartTimestamp: now,
				EndTimestamp:   now.Add(10 * time.Millisecond),
			},
			durable.OperationHookInfo{
				ExecutionArn:   arn,
				ID:             "child-op",
				Name:           "child",
				Type:           "STEP",
				Status:         durable.PluginOperationSucceeded,
				ParentID:       "parent-op",
				StartTimestamp: now,
				EndTimestamp:   now.Add(5 * time.Millisecond),
			},
		),
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

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})

	// The EXECUTION operation should be skipped.
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
		Operations: opsByID(durable.OperationHookInfo{
			ExecutionArn: arn,
			ID:           "exec-root",
			Type:         "EXECUTION",
			Status:       durable.PluginOperationStarted,
		}),
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if len(records[0].Operations) != 0 {
		t.Errorf("expected 0 operations (EXECUTION excluded), got %d", len(records[0].Operations))
	}
}

func TestPlugin_CreatePlugin_InitializesMetadataAndInput(t *testing.T) {
	ctx := t.Context()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/name/id"
	var redacted []any
	content := ContentConfig{InputTransform: func(v any) any {
		// v is the decoded JSON value, so the input string arrives as a
		// Go string.
		redacted = append(redacted, v)
		s, _ := v.(string)
		return "redacted:" + s
	}}
	exp := &capturingExporter{}
	factory := New(Config{EmitMode: EmitAlways, Content: content, Exporters: []Exporter{exp}})
	info := durable.InvocationHookInfo{ExecutionArn: arn, ExecutionInput: "input", ExecutionStartTimestamp: time.Now().Add(-time.Hour)}
	plugin := createTestPlugin(t, ctx, factory, info)
	if len(redacted) != 1 || redacted[0] != "input" {
		t.Fatalf("input was not prepared by CreatePlugin: %v", redacted)
	}
	fields := plugin.EnrichLogContext(ctx)
	if fields["execution_arn"] != arn || fields["function_name"] != "fn" {
		t.Errorf("log metadata before OnInvocationStart = %v", fields)
	}
	plugin.OnInvocationStart(ctx, info)
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{Status: durable.PluginInvocationPending})
	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want one", len(records))
	}
	rec := records[0]
	if string(rec.Input) != `"redacted:input"` || len(redacted) != 1 || rec.StartTimestamp == nil || !rec.StartTimestamp.Equal(info.ExecutionStartTimestamp) || rec.Status != StatusRunning {
		t.Errorf("initialized record = %+v, redactor calls = %v", rec, redacted)
	}
}

// TestPlugin_FinalRecordUsesInvocationEndOperations asserts that the final
// record is built from the operations of OnInvocationEnd. The last change
// the plugin received reports the step as STARTED. The SDK drops the change
// that reports it as SUCCEEDED when the invocation's context ends first.
// OnInvocationEnd then carries the step as SUCCEEDED. The final record
// holds the SUCCEEDED status, the end timestamp, and the attempt.
func TestPlugin_FinalRecordUsesInvocationEndOperations(t *testing.T) {
	for _, mode := range []EmitMode{EmitOnComplete, EmitAlways, EmitOnChange} {
		t.Run(string(mode), func(t *testing.T) {
			exp := &capturingExporter{}
			ip := New(Config{Exporters: []Exporter{exp}, EmitMode: mode})

			ctx := context.Background()
			arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"
			now := time.Now()

			plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
				ExecutionArn:            arn,
				ExecutionStartTimestamp: now,
			})

			step := durable.OperationHookInfo{ExecutionArn: arn, ID: "op-1", Name: "step", Type: "STEP", Status: durable.PluginOperationStarted, StartTimestamp: now}
			reportChange(ctx, plugin, step)

			succeeded := step
			succeeded.Status = durable.PluginOperationSucceeded
			succeeded.Attempt = 1
			succeeded.EndTimestamp = now.Add(time.Millisecond)
			plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
				ExecutionArn:    arn,
				Status:          durable.PluginInvocationSucceeded,
				ExecutionResult: "done",
				Operations:      opsByID(succeeded),
			})

			records := exp.snapshot()
			if len(records) == 0 {
				t.Fatal("no record was exported")
			}
			final := records[len(records)-1]
			if final.Status != StatusSucceeded || len(final.Operations) != 1 {
				t.Fatalf("final record = %+v, want SUCCEEDED with 1 operation", final)
			}
			if op := final.Operations[0]; op.Status != string(durable.PluginOperationSucceeded) || op.Attempt != 1 || op.EndTimestamp == nil {
				t.Errorf("operation = %+v, want the SUCCEEDED record with attempt 1 and an end timestamp", op)
			}
		})
	}
}

// TestPlugin_OperationError_CheckpointedTypeAndMessage asserts the error of
// an operation in the record. A failed operation whose snapshot entry holds
// a *durable.OperationError reports the checkpointed error type and
// message. When the checkpointed error has no type, the type in the record
// is "Error". A failed operation whose snapshot entry holds no error, for
// example a callback whose caller sent none, has no error in the record.
func TestPlugin_OperationError_CheckpointedTypeAndMessage(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"
	now := time.Now()

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:   arn,
		Status:         durable.PluginInvocationFailed,
		ExecutionError: &testError{msg: "handler failed"},
		Operations: opsByID(
			durable.OperationHookInfo{
				ID: "op-1", Name: "charge", Type: "STEP", Status: durable.PluginOperationFailed,
				StartTimestamp: now,
				Error:          checkpointedError("charge", "PaymentDeclinedError", "card expired"),
			},
			durable.OperationHookInfo{
				ID: "op-2", Name: "approve", Type: "CALLBACK", Status: durable.PluginOperationFailed,
				StartTimestamp: now.Add(time.Millisecond),
			},
			durable.OperationHookInfo{
				ID: "op-3", Name: "confirm", Type: "CALLBACK", Status: durable.PluginOperationFailed,
				StartTimestamp: now.Add(2 * time.Millisecond),
				Error:          checkpointedError("confirm", "", "rejected"),
			},
		),
	})

	records := exp.snapshot()
	if len(records) != 1 || len(records[0].Operations) != 3 {
		t.Fatalf("records = %+v, want 1 record with 3 operations", records)
	}
	step, callback, untyped := records[0].Operations[0], records[0].Operations[1], records[0].Operations[2]
	if untyped.Error == nil || untyped.Error.Type != "Error" || untyped.Error.Message != "rejected" {
		t.Errorf("untyped error = %+v, want type Error and message rejected", untyped.Error)
	}
	if step.Error == nil || step.Error.Type != "PaymentDeclinedError" || step.Error.Message != "card expired" {
		t.Errorf("step error = %+v, want type PaymentDeclinedError and message card expired", step.Error)
	}
	if callback.Status != string(durable.PluginOperationFailed) || callback.Error != nil {
		t.Errorf("callback = %+v, want FAILED with no error", callback)
	}
}

// TestPlugin_DurablePlugin_WiresHooksCorrectly asserts the exact set of
// hooks that a sampled-in invocation registers in each emit mode.
// CreatePlugin returns a separate durable.Plugin literal for EmitOnChange.
// So the test compares every func field of the plugin with the expected
// set, and a hook missing from one literal fails it.
func TestPlugin_DurablePlugin_WiresHooksCorrectly(t *testing.T) {
	base := []string{"EnrichLogContext", "OnInvocationEnd", "OnInvocationStart"}
	tests := []struct {
		mode EmitMode
		want []string
	}{
		{EmitOnComplete, base},
		{EmitAlways, base},
		{EmitOnChange, []string{"EnrichLogContext", "OnInvocationEnd", "OnInvocationStart", "OnOperationChange"}},
	}
	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			dp, err := New(Config{EmitMode: tt.mode}).CreatePlugin(t.Context(), durable.InvocationHookInfo{})
			if err != nil {
				t.Fatalf("CreatePlugin returned error: %v", err)
			}
			var got []string
			fields := reflect.ValueOf(*dp)
			for i := range fields.NumField() {
				if fields.Field(i).Kind() == reflect.Func && !fields.Field(i).IsNil() {
					got = append(got, fields.Type().Field(i).Name)
				}
			}
			slices.Sort(got)
			if !slices.Equal(got, tt.want) {
				t.Errorf("registered hooks = %v, want %v", got, tt.want)
			}
		})
	}
}

// testError is a simple error type for testing.
type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }
