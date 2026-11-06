package telemetry

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

const resumeTestARN = "arn:aws:lambda:us-east-1:123456789012:function:order-processor:1/durable-execution/exec-resume/exec-id-1"

// resumeInvocation creates a fresh invocation plugin through the factory
// and calls its OnInvocationStart hook, as the handler does.
func resumeInvocation(t *testing.T, factory durable.PluginFactory, info durable.InvocationHookInfo) *durable.Plugin {
	t.Helper()
	ctx := context.Background()
	pl, err := factory.CreatePlugin(ctx, info)
	if err != nil {
		t.Fatalf("CreatePlugin: %v", err)
	}
	if pl == nil {
		t.Fatal("CreatePlugin returned a nil plugin")
	}
	if pl.OnInvocationStart != nil {
		pl.OnInvocationStart(ctx, info)
	}
	return pl
}

func resumeOp(id, parentID, typ string, status durable.PluginOperationStatus, start time.Time) durable.OperationHookInfo {
	return durable.OperationHookInfo{
		ExecutionArn:   resumeTestARN,
		ID:             id,
		Name:           id,
		Type:           typ,
		Status:         status,
		ParentID:       parentID,
		StartTimestamp: start,
	}
}

// TestResume_PreservesStartTimestampInputAndOutput verifies that a plugin
// created for a resumed invocation reports the original execution start
// timestamp and execution input, and records the final output.
func TestResume_PreservesStartTimestampInputAndOutput(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{EmitMode: EmitAlways, Exporters: []Exporter{exp}})
	ctx := context.Background()
	start := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	input := map[string]string{"orderId": "abc"}

	first := resumeInvocation(t, factory, durable.InvocationHookInfo{
		ExecutionArn:            resumeTestARN,
		RequestID:               "req-1",
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: start,
		ExecutionInput:          input,
	})
	first.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn: resumeTestARN,
		Status:       durable.PluginInvocationPending,
	})

	second := resumeInvocation(t, factory, durable.InvocationHookInfo{
		ExecutionArn:            resumeTestARN,
		RequestID:               "req-2",
		ExecutionStartTimestamp: start,
		ExecutionInput:          input,
	})
	second.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    resumeTestARN,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "done",
	})

	records := exp.snapshot()
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	pending, final := records[0], records[1]

	if pending.Status != StatusRunning || pending.EndTimestamp != nil || pending.DurationMs != nil {
		t.Errorf("pending record = status %s end %v duration %v, want RUNNING with no end or duration",
			pending.Status, pending.EndTimestamp, pending.DurationMs)
	}
	if final.Status != StatusSucceeded {
		t.Errorf("final status = %s, want SUCCEEDED", final.Status)
	}
	for name, rec := range map[string]Record{"pending": pending, "final": final} {
		if rec.ExecutionArn != resumeTestARN {
			t.Errorf("%s ExecutionArn = %q, want %q", name, rec.ExecutionArn, resumeTestARN)
		}
		if rec.FunctionName != "order-processor" {
			t.Errorf("%s FunctionName = %q, want order-processor", name, rec.FunctionName)
		}
		if rec.StartTimestamp == nil || !rec.StartTimestamp.Equal(start) {
			t.Errorf("%s StartTimestamp = %v, want original %v", name, rec.StartTimestamp, start)
		}
		if rec.Input == nil || rec.Input.Value != `{"orderId":"abc"}` {
			t.Errorf("%s Input = %+v, want {\"orderId\":\"abc\"}", name, rec.Input)
		}
	}
	if final.Output == nil || final.Output.Value != "done" {
		t.Errorf("final Output = %+v, want done", final.Output)
	}
	if final.DurationMs == nil || *final.DurationMs < time.Hour.Milliseconds() {
		t.Errorf("final DurationMs = %v, want at least one hour measured from the original start", final.DurationMs)
	}
}

// TestResume_FailedTerminalOutcome verifies that a resumed invocation
// that fails records the error and the original start timestamp.
func TestResume_FailedTerminalOutcome(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{Exporters: []Exporter{exp}})
	start := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	resumed := resumeInvocation(t, factory, durable.InvocationHookInfo{
		ExecutionArn:            resumeTestARN,
		RequestID:               "req-2",
		ExecutionStartTimestamp: start,
	})
	resumed.OnInvocationEnd(context.Background(), durable.InvocationEndHookInfo{
		ExecutionArn:   resumeTestARN,
		Status:         durable.PluginInvocationFailed,
		ExecutionError: errors.New("boom"),
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	rec := records[0]
	if rec.Status != StatusFailed || rec.Error == nil || rec.Error.Message != "boom" {
		t.Errorf("record = status %s error %+v, want FAILED with message boom", rec.Status, rec.Error)
	}
	if rec.StartTimestamp == nil || !rec.StartTimestamp.Equal(start) {
		t.Errorf("StartTimestamp = %v, want original %v", rec.StartTimestamp, start)
	}
}

// resumeEmits runs one complete invocation of the execution through the
// factory and reports whether any record was exported.
func resumeEmits(t *testing.T, factory durable.PluginFactory, exp *capturingExporter, arn, requestID string) bool {
	t.Helper()
	before := len(exp.snapshot())
	info := durable.InvocationHookInfo{
		ExecutionArn:            arn,
		RequestID:               requestID,
		ExecutionStartTimestamp: time.Now(),
	}
	pl := resumeInvocation(t, factory, info)
	pl.OnInvocationEnd(context.Background(), durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})
	return len(exp.snapshot()) > before
}

// TestResume_SamplingDecisionIsStableAcrossInvocations verifies that the
// sampling decision depends on the execution ARN alone. A sampled-in
// execution emits on every invocation, and a sampled-out execution emits
// on none, whatever the request ID.
func TestResume_SamplingDecisionIsStableAcrossInvocations(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{EmitMode: EmitAlways, SamplingRate: 0.5, Exporters: []Exporter{exp}})

	// Find one ARN the factory samples in and one it samples out. The
	// probe observes only whether a record is exported.
	var inARN, outARN string
	for i := 0; i < 1000 && (inARN == "" || outARN == ""); i++ {
		arn := "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/probe-" + strconv.Itoa(i) + "/id"
		if resumeEmits(t, factory, exp, arn, "probe") {
			if inARN == "" {
				inARN = arn
			}
		} else if outARN == "" {
			outARN = arn
		}
	}
	if inARN == "" || outARN == "" {
		t.Fatalf("could not find a sampled-in and a sampled-out ARN (in=%q out=%q)", inARN, outARN)
	}

	for i := 0; i < 5; i++ {
		req := "req-" + strconv.Itoa(i)
		if !resumeEmits(t, factory, exp, inARN, req) {
			t.Errorf("sampled-in execution did not emit on invocation with request ID %s", req)
		}
		if resumeEmits(t, factory, exp, outARN, req) {
			t.Errorf("sampled-out execution emitted on invocation with request ID %s", req)
		}
	}
}

// resumeSnapshot builds the Operations and UpdatedOperations maps of a
// resumed invocation. The child context "child" completed in an earlier
// invocation and holds a completed step. The wait "pause" completed
// while the execution was suspended, so it is the only updated operation.
func resumeSnapshot(start time.Time) (ops, updated map[string]durable.OperationHookInfo) {
	root := resumeOp("root", "", "EXECUTION", durable.PluginOperationStarted, start)
	child := resumeOp("child", "", "CONTEXT", durable.PluginOperationSucceeded, start.Add(time.Second))
	nested := resumeOp("nested", "child", "STEP", durable.PluginOperationSucceeded, start.Add(2*time.Second))
	nested.Result = `"nested-result"`
	pause := resumeOp("pause", "", "WAIT", durable.PluginOperationSucceeded, start.Add(3*time.Second))
	for _, op := range []*durable.OperationHookInfo{&child, &nested, &pause} {
		op.IsReplay = true
	}
	ops = map[string]durable.OperationHookInfo{"root": root, "child": child, "nested": nested, "pause": pause}
	updated = map[string]durable.OperationHookInfo{"pause": pause}
	return ops, updated
}

func resumeOperationIDs(rec Record) map[string]int {
	counts := map[string]int{}
	for _, op := range rec.Operations {
		counts[op.ID]++
	}
	return counts
}

// TestResume_FullTreeIncludesUnchangedOperationsFromSnapshot verifies that
// a full-tree record includes unchanged checkpointed operations from the
// start snapshot, excludes the root execution operation, and does not
// duplicate an operation that a later hook reports again.
func TestResume_FullTreeIncludesUnchangedOperationsFromSnapshot(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{
		EmitMode:        EmitOnComplete,
		OperationDetail: OperationDetailFullTree,
		Exporters:       []Exporter{exp},
	})
	ctx := context.Background()
	start := time.Now().Add(-time.Minute)
	ops, updated := resumeSnapshot(start)

	pl := resumeInvocation(t, factory, durable.InvocationHookInfo{
		ExecutionArn:            resumeTestARN,
		RequestID:               "req-2",
		ExecutionStartTimestamp: start,
		Operations:              ops,
		UpdatedOperations:       updated,
	})

	// A new operation starts and ends, and an operation from the
	// snapshot is reported again.
	late := resumeOp("late", "", "STEP", durable.PluginOperationStarted, start.Add(4*time.Second))
	pl.OnOperationStart(ctx, late)
	late.Status = durable.PluginOperationSucceeded
	late.Result = `"late-result"`
	pl.OnOperationEnd(ctx, late)
	pl.OnOperationEnd(ctx, ops["pause"])

	pl.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    resumeTestARN,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	counts := resumeOperationIDs(records[0])
	for _, id := range []string{"child", "nested", "pause", "late"} {
		if counts[id] != 1 {
			t.Errorf("operation %q appears %d times, want 1 (operations: %v)", id, counts[id], counts)
		}
	}
	if counts["root"] != 0 {
		t.Errorf("root execution operation appears %d times, want 0", counts["root"])
	}
	if len(records[0].Operations) != 4 {
		t.Errorf("record has %d operations, want 4: %v", len(records[0].Operations), counts)
	}
	for _, op := range records[0].Operations {
		if op.ID == "nested" {
			if op.ParentID != "child" || op.Result == nil || op.Result.Value != `"nested-result"` {
				t.Errorf("nested operation = %+v, want parent child and result \"nested-result\"", op)
			}
		}
		if op.ID == "late" && (op.Result == nil || op.Result.Value != `"late-result"`) {
			t.Errorf("late operation = %+v, want the result from its end hook", op)
		}
	}
}

// TestResume_TopLevelDetailOmitsNestedSnapshotOperations verifies that the
// top-level detail still drops nested operations from the start snapshot.
func TestResume_TopLevelDetailOmitsNestedSnapshotOperations(t *testing.T) {
	exp := &capturingExporter{}
	factory := New(Config{
		EmitMode:        EmitOnComplete,
		OperationDetail: OperationDetailTopLevel,
		Exporters:       []Exporter{exp},
	})
	start := time.Now().Add(-time.Minute)
	ops, updated := resumeSnapshot(start)

	pl := resumeInvocation(t, factory, durable.InvocationHookInfo{
		ExecutionArn:            resumeTestARN,
		RequestID:               "req-2",
		ExecutionStartTimestamp: start,
		Operations:              ops,
		UpdatedOperations:       updated,
	})
	pl.OnInvocationEnd(context.Background(), durable.InvocationEndHookInfo{
		ExecutionArn:    resumeTestARN,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: "ok",
	})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	counts := resumeOperationIDs(records[0])
	if counts["child"] != 1 || counts["pause"] != 1 || counts["nested"] != 0 || counts["root"] != 0 || len(records[0].Operations) != 2 {
		t.Errorf("top-level operations = %v, want child and pause once", counts)
	}
}
