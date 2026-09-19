package insight

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func TestPlugin_EmitOnChange_EmitsRunningSnapshotsPlusFinal(t *testing.T) {
	exp := &capturingExporter{}
	ip := New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}})

	ctx := context.Background()
	now := time.Now()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	// Step 1 start+end.
	ip.onOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now.Add(5 * time.Millisecond),
	})
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(5 * time.Millisecond),
		EndTimestamp:   now.Add(15 * time.Millisecond),
	})

	// Step 2 start+end.
	ip.onOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-2",
		Name:           "step-2",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now.Add(20 * time.Millisecond),
	})
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-2",
		Name:           "step-2",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(20 * time.Millisecond),
		EndTimestamp:   now.Add(30 * time.Millisecond),
	})

	// Wait for on-change dispatches to complete before final emit.
	ip.onChangeWG.Wait()

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	ip.onOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now,
	})
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	ip.onOperationStart(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now,
	})
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: time.Now(),
	})
	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
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

// TestPlugin_RetryingInvocation_LeavesRecordRunning verifies that a
// retrying outcome is handled like a pending one in every emit mode: the
// execution continues in a later invocation, so no record is marked
// terminal, no end timestamp is set, and the invocation's error is not
// recorded as the execution's error. EmitOnComplete emits nothing;
// EmitAlways and EmitOnChange emit one running snapshot.
func TestPlugin_RetryingInvocation_LeavesRecordRunning(t *testing.T) {
	cases := []struct {
		name        string
		mode        EmitMode
		wantRecords int
	}{
		{name: "default EmitOnComplete", mode: EmitOnComplete, wantRecords: 0},
		{name: "EmitAlways", mode: EmitAlways, wantRecords: 1},
		{name: "EmitOnChange", mode: EmitOnChange, wantRecords: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exp := &capturingExporter{}
			ip := New(Config{EmitMode: tc.mode, Exporters: []Exporter{exp}})

			ctx := context.Background()
			arn := "arn:aws:lambda:us-east-1:123:function:fn:1"

			ip.onInvocationStart(ctx, durable.InvocationHookInfo{
				ExecutionArn:            arn,
				ExecutionStartTimestamp: time.Now(),
			})
			ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
				ExecutionArn:   arn,
				Status:         durable.PluginInvocationRetrying,
				ExecutionError: errors.New("transient"),
			})

			records := exp.snapshot()
			if len(records) != tc.wantRecords {
				t.Fatalf("got %d records for a retrying invocation, want %d", len(records), tc.wantRecords)
			}
			for _, rec := range records {
				if rec.Status != StatusRunning {
					t.Errorf("record status = %q, want %q", rec.Status, StatusRunning)
				}
				if rec.EndTimestamp != nil {
					t.Errorf("record end timestamp = %v, want nil for a still-running execution", rec.EndTimestamp)
				}
				if rec.DurationMs != nil {
					t.Errorf("record duration = %v, want nil for a still-running execution", *rec.DurationMs)
				}
				if rec.Error != nil {
					t.Errorf("record error = %+v, want nil: the execution has not failed", rec.Error)
				}
			}

			// The cumulative record must also stay running, so the next
			// invocation continues it rather than starting from a
			// finalized one.
			ip.mu.Lock()
			defer ip.mu.Unlock()
			if ip.record == nil {
				t.Fatal("cumulative record is nil after a retrying invocation")
			}
			if ip.record.Status != StatusRunning {
				t.Errorf("cumulative record status = %q, want %q", ip.record.Status, StatusRunning)
			}
			if ip.record.EndTimestamp != nil {
				t.Errorf("cumulative record end timestamp = %v, want nil", ip.record.EndTimestamp)
			}
		})
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
	// Regression test: under OperationDetailFullTree + OperationFilter,
	// ApplyToRecord's filter-in-place must not corrupt p.record's backing
	// array (which would cause subsequent on-change snapshots to lose data).
	exp := &capturingExporter{}
	ip := New(Config{
		EmitMode:        EmitOnChange,
		OperationDetail: OperationDetailFullTree,
		Exporters:       []Exporter{exp},
		Content: ContentConfig{
			IncludeInput:            true,
			IncludeOutput:           true,
			IncludeOperationResults: true,
			IncludeOperationErrors:  true,
			IncludeExecutionError:   true,
			// Filter that excludes "filtered-out" operations.
			OperationFilter: func(op OperationRecord) bool {
				return op.Name != "filtered-out"
			},
		},
	})

	ctx := context.Background()
	now := time.Now()
	arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

	ip.onInvocationStart(ctx, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		ExecutionStartTimestamp: now,
	})

	// Add a "keep" op.
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-keep",
		Name:           "keep-me",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	})

	// Wait for first on-change dispatch.
	ip.onChangeWG.Wait()

	// Add the filtered-out op.
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-filter",
		Name:           "filtered-out",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(15 * time.Millisecond),
		EndTimestamp:   now.Add(20 * time.Millisecond),
	})

	// Wait for second on-change dispatch.
	ip.onChangeWG.Wait()

	// Add another "keep" op.
	ip.onOperationEnd(ctx, durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-keep-2",
		Name:           "keep-me-2",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(25 * time.Millisecond),
		EndTimestamp:   now.Add(30 * time.Millisecond),
	})

	// Wait for third on-change dispatch.
	ip.onChangeWG.Wait()

	ip.onInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "done",
	})

	records := exp.snapshot()
	if len(records) < 2 {
		t.Fatalf("expected at least 2 records, got %d", len(records))
	}

	// The final record should have exactly 2 "keep" ops (the
	// "filtered-out" op is excluded by OperationFilter).
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
