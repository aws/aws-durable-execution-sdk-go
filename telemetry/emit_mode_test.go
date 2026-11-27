package telemetry

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// TestPlugin_EmitOnChange_EmissionPoints asserts the three points at which
// EmitOnChange emits a record: at invocation start, after each
// OnOperationChange, and at invocation end. The test waits for each
// emission before the next hook, so no two emissions are coalesced. Each
// record lists the full set of operations its hook carried. The record at
// invocation end is terminal when the execution finished, and RUNNING when
// the invocation ended as pending.
func TestPlugin_EmitOnChange_EmissionPoints(t *testing.T) {
	cases := []struct {
		name       string
		end        durable.PluginInvocationStatus
		wantStatus Status
	}{
		{name: "succeeded", end: durable.PluginInvocationSucceeded, wantStatus: StatusSucceeded},
		{name: "pending", end: durable.PluginInvocationPending, wantStatus: StatusRunning},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exp := &capturingExporter{exported: make(chan struct{}, 16)}
			ip := New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}})

			ctx := context.Background()
			now := time.Now()
			arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1"

			// An earlier invocation checkpointed step-1.
			step1 := durable.OperationHookInfo{
				ExecutionArn: arn, ID: "op-1", Name: "step-1", Type: "STEP", Status: durable.PluginOperationSucceeded,
				StartTimestamp: now.Add(5 * time.Millisecond), EndTimestamp: now.Add(15 * time.Millisecond),
			}
			plugin := startTestPlugin(t, ctx, ip, durable.InvocationHookInfo{
				ExecutionArn:            arn,
				ExecutionStartTimestamp: now,
				Operations:              opsByID(step1),
			})
			awaitSignal(t, exp.exported)

			// A checkpoint response reports step-2 as STARTED.
			step2 := durable.OperationHookInfo{
				ExecutionArn: arn, ID: "op-2", Name: "step-2", Type: "STEP", Status: durable.PluginOperationStarted,
				StartTimestamp: now.Add(20 * time.Millisecond),
			}
			reportChange(ctx, plugin, step1, step2)
			awaitSignal(t, exp.exported)

			// The next response reports step-2 as SUCCEEDED.
			step2.Status = durable.PluginOperationSucceeded
			step2.EndTimestamp = now.Add(30 * time.Millisecond)
			reportChange(ctx, plugin, step1, step2)
			awaitSignal(t, exp.exported)

			plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
				ExecutionArn:    arn,
				Status:          tc.end,
				ExecutionResult: "done",
				Operations:      opsByID(step1, step2),
			})

			records := exp.snapshot()
			if len(records) != 4 {
				t.Fatalf("got %d records, want 4: start, two changes, end", len(records))
			}
			want := []struct {
				status   Status
				statuses []string // operation statuses, in record order
			}{
				{StatusRunning, []string{"SUCCEEDED"}},
				{StatusRunning, []string{"SUCCEEDED", "STARTED"}},
				{StatusRunning, []string{"SUCCEEDED", "SUCCEEDED"}},
				{tc.wantStatus, []string{"SUCCEEDED", "SUCCEEDED"}},
			}
			for i, rec := range records {
				if rec.Status != want[i].status {
					t.Errorf("record %d status = %s, want %s", i, rec.Status, want[i].status)
				}
				var got []string
				for _, op := range rec.Operations {
					got = append(got, op.Status)
				}
				if !slices.Equal(got, want[i].statuses) {
					t.Errorf("record %d operation statuses = %v, want %v", i, got, want[i].statuses)
				}
				if i > 0 && !rec.EmittedAt.After(records[i-1].EmittedAt) {
					t.Errorf("record %d EmittedAt %v is not after record %d EmittedAt %v", i, rec.EmittedAt, i-1, records[i-1].EmittedAt)
				}
			}
		})
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

	step := durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now,
	}
	reportChange(ctx, plugin, step)
	step.Status = durable.PluginOperationSucceeded
	step.EndTimestamp = now.Add(10 * time.Millisecond)
	reportChange(ctx, plugin, step)

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
		Operations:      opsByID(step),
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

	step := durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-1",
		Name:           "step-1",
		Type:           "STEP",
		Status:         durable.PluginOperationStarted,
		StartTimestamp: now,
	}
	reportChange(ctx, plugin, step)
	step.Status = durable.PluginOperationSucceeded
	step.EndTimestamp = now.Add(10 * time.Millisecond)
	reportChange(ctx, plugin, step)

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
		Operations:      opsByID(step),
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
				// EmitOnComplete emits nothing for a continuing
				// invocation. EmitAlways emits at invocation end.
				// EmitOnChange emits at invocation start and end.
				wantRecords := map[EmitMode]int{EmitOnComplete: 0, EmitAlways: 1, EmitOnChange: 2}[mode]
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

	// Observe the emission at invocation start. It lists no operation.
	awaitSignal(t, exp.exported)

	keep := durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-keep",
		Name:           "keep-me",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now,
		EndTimestamp:   now.Add(10 * time.Millisecond),
	}
	filtered := durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-filter",
		Name:           "filtered-out",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(15 * time.Millisecond),
		EndTimestamp:   now.Add(20 * time.Millisecond),
	}
	keep2 := durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "op-keep-2",
		Name:           "keep-me-2",
		Type:           "STEP",
		Status:         durable.PluginOperationSucceeded,
		StartTimestamp: now.Add(25 * time.Millisecond),
		EndTimestamp:   now.Add(30 * time.Millisecond),
	}

	// Each change carries one more operation. Observe the on-change
	// emission of each before the next.
	reportChange(ctx, plugin, keep)
	awaitSignal(t, exp.exported)
	reportChange(ctx, plugin, keep, filtered)
	awaitSignal(t, exp.exported)
	reportChange(ctx, plugin, keep, filtered, keep2)
	awaitSignal(t, exp.exported)

	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "done",
		Operations:      opsByID(keep, filtered, keep2),
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
	// is still present in every running record emitted after a change.
	// Record 0 is the emission at invocation start.
	for i, rec := range records[1 : len(records)-1] {
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
