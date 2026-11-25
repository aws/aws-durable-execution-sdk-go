package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func TestPlugin_EmitOnChange_EmitsRunningSnapshotsPlusFinal(t *testing.T) {
	exp := &capturingExporter{exported: make(chan struct{}, 16)}
	ip := New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}})

	ctx := context.Background()
	now := time.Now()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	// Step 1 start+end.
	plugin.OnOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now.Add(5 * time.Millisecond),
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(5 * time.Millisecond),
		EndTimestamp:   now.Add(15 * time.Millisecond),
	})

	// Step 2 start+end.
	plugin.OnOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-2",
		Name:           "step-2",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now.Add(20 * time.Millisecond),
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-2",
		Name:           "step-2",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(20 * time.Millisecond),
		EndTimestamp:   now.Add(30 * time.Millisecond),
	})

	// Observe a running emission before ending the invocation.
	awaitSignal(t, exp.exported)

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "done",
	})

	records := exp.snapshot()
	if len(records) < 2 {
		t.Fatalf("expected at least 2 records (running + final), got %d", len(records))
	}

	last := records[len(records)-1]
	if last.Status != StatusSucceeded {
		t.Errorf("expected last record Status=SUCCEEDED, got %s", last.Status)
	}

	sawRunning := false
	for _, rec := range records[:len(records)-1] {
		if rec.Status == StatusRunning {
			sawRunning = true
		}
	}
	if !sawRunning {
		t.Error("expected at least one RUNNING snapshot before the final record")
	}
}

func TestPlugin_EmitOnComplete_NeverEmitsRunningSnapshots(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{Exporters: []Exporter{exp}}) // default: EmitOnComplete

	ctx := context.Background()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"
	now := time.Now()

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	plugin.OnOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now,
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 record under EmitOnComplete, got %d", len(records))
	}
	if records[0].Status != StatusSucceeded {
		t.Errorf("expected Status=SUCCEEDED, got %s", records[0].Status)
	}
}

func TestPlugin_EmitOnChange_SampledOut_EmitsNothing(t *testing.T) {
	exp := &capturingExporter{}
	arn := "arn:aws:lambda:us-east-1:123456789012:function:f:1"
	rate := 0.0000001
	if sampledIn(arn, rate) {
		t.Skip("this ARN samples in at this rate")
	}
	ip := New(Config{EmitMode: EmitOnChange, SamplingRate: rate, Exporters: []Exporter{exp}})

	ctx := context.Background()
	now := time.Now()

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	plugin.OnOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now,
	})
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	if records := exp.snapshot(); len(records) != 0 {
		t.Fatalf("expected 0 records (sampled out), got %d", len(records))
	}
}

func TestPlugin_EmitAlways_EmitsForSucceededExecution(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{EmitMode: EmitAlways, Exporters: []Exporter{exp}})

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

	if records := exp.snapshot(); len(records) != 1 {
		t.Fatalf("expected 1 record under EmitAlways, got %d", len(records))
	}
}

// TestPlugin_EmitAlways_EmitsForPendingInvocation verifies that EmitAlways
// emits a running snapshot when an invocation suspends: the record keeps
// the running status with no end timestamp, since the execution continues.
func TestPlugin_EmitAlways_EmitsForPendingInvocation(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{EmitMode: EmitAlways, Exporters: []Exporter{exp}})

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

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record for pending invocation under EmitAlways, got %d", len(records))
	}
	if records[0].Status != StatusRunning {
		t.Errorf("record status = %q, want %q", records[0].Status, StatusRunning)
	}
	if records[0].EndTimestamp != nil {
		t.Errorf("record end timestamp = %v, want nil for a still-running execution", records[0].EndTimestamp)
	}
}

// Pending and retrying invocations continue the execution. Their emissions
// must remain RUNNING and must not expose a transient error as a terminal one.
func TestPlugin_ContinuingInvocation_LeavesRecordRunning(t *testing.T) {
	for _, status := range []durable.PluginInvocationStatus{durable.PluginInvocationPending, durable.PluginInvocationRetrying} {
		for _, mode := range []EmitMode{EmitOnComplete, EmitAlways, EmitOnChange} {
			t.Run(string(status)+"/"+string(mode), func(t *testing.T) {
				exp := &capturingExporter{}
				ctx := t.Context()
				factory := New(Config{EmitMode: mode, Exporters: []Exporter{exp}})
				plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
					ExecutionArn:            "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/name/id",
					ExecutionStartTimestamp: time.Now(),
				})
				plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
					Status:         status,
					ExecutionError: errors.New("transient"),
				})
				wantRecords := 1
				if mode == EmitOnComplete {
					wantRecords = 0
				}
				records := exp.snapshot()
				if len(records) != wantRecords {
					t.Fatalf("got %d records, want %d", len(records), wantRecords)
				}
				for _, rec := range records {
					if rec.Status != StatusRunning || rec.EndTimestamp != nil || rec.DurationMs != nil || rec.Error != nil || rec.Output != nil {
						t.Errorf("continuing invocation record = %+v", rec)
					}
				}
			})
		}
	}
}

func TestNew_UnrecognizedEmitMode_DefaultsToOnComplete(t *testing.T) {
	ip := New(Config{EmitMode: "not-a-real-mode"})
	if ip.emitMode != EmitOnComplete {
		t.Errorf("expected unrecognized EmitMode to default to EmitOnComplete, got %v", ip.emitMode)
	}
}

func TestNew_EmitOnChange_Accepted(t *testing.T) {
	ip := New(Config{EmitMode: EmitOnChange})
	if ip.emitMode != EmitOnChange {
		t.Errorf("expected EmitOnChange to be accepted, got %v", ip.emitMode)
	}
}

func TestPlugin_EmitOnChange_FullTree_ContentFilter_NoBackingArrayCorruption(t *testing.T) {
	// Regression test: under OperationDetailFullTree with an Exclude
	// override, dropping operations at snapshot time must not corrupt
	// p.record's backing array (which would cause subsequent on-change
	// snapshots to lose data).
	exp := &capturingExporter{exported: make(chan struct{}, 16)}
	ip := New(Config{
		EmitMode:        EmitOnChange,
		OperationDetail: OperationDetailFullTree,
		Exporters:       []Exporter{exp},
		Content: ContentConfig{
			Operations: ContentOperations{
				Overrides: []OperationOverride{{OperationName: "filtered-out", Exclude: true}},
			},
		},
	})

	ctx := context.Background()
	now := time.Now()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	// Add a "keep" op.
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-keep",
		Name:           "keep-me",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	// Observe the first on-change emission.
	awaitSignal(t, exp.exported)

	// Add the filtered-out op.
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-filter",
		Name:           "filtered-out",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(15 * time.Millisecond),
		EndTimestamp:   now.Add(20 * time.Millisecond),
	})

	// Observe the next on-change emission.
	awaitSignal(t, exp.exported)

	// Add another "keep" op.
	plugin.OnOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-keep-2",
		Name:           "keep-me-2",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(25 * time.Millisecond),
		EndTimestamp:   now.Add(30 * time.Millisecond),
	})

	// Observe the next on-change emission.
	awaitSignal(t, exp.exported)

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "done",
	})

	records := exp.snapshot()
	if len(records) < 2 {
		t.Fatalf("expected at least 2 records, got %d", len(records))
	}

	// The final record should have exactly 2 "keep" ops (the
	// "filtered-out" op is excluded by the override).
	last := records[len(records)-1]
	if last.Status != StatusSucceeded {
		t.Errorf("expected final Status=SUCCEEDED, got %s", last.Status)
	}
	keepCount := 0
	for _, op := range last.Operations {
		if op.Name == "filtered-out" {
			t.Errorf("filtered-out operation should not appear in emitted record")
		}
		if op.Name == "keep-me" || op.Name == "keep-me-2" {
			keepCount++
		}
	}
	if keepCount != 2 {
		t.Errorf("expected 2 keep ops in final record, got %d (ops: %+v)", keepCount, last.Operations)
	}

	// Verify the internal state was not corrupted: check that "keep-me"
	// is still present in all emitted running records.
	for i, rec := range records[:len(records)-1] {
		hasKeep := false
		for _, op := range rec.Operations {
			if op.Name == "keep-me" {
				hasKeep = true
			}
		}
		if !hasKeep {
			t.Errorf("running record %d lost 'keep-me' op — backing array corrupted", i)
		}
	}
}
