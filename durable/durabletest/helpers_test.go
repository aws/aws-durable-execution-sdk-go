// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// --- Reset ---

func TestResetRunsWorkflowFromScratch(t *testing.T) {
	var executions atomic.Int32
	handler := func(ctx durable.Context, event string) (string, error) {
		n, err := durable.Step(ctx, "count", func(_ durable.StepContext) (int32, error) {
			return executions.Add(1), nil
		})
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "pause", time.Minute); err != nil {
			return "", err
		}
		return strings.Repeat(event, int(n)), nil
	}

	runner := durabletest.NewLocalRunner(handler)

	first := runner.RunUntilComplete(t, "x")
	if first.Status != durabletest.Succeeded {
		t.Fatalf("first run: status = %s, want SUCCEEDED", first.Status)
	}
	if got, _ := durabletest.ResultAs[string](first); got != "x" {
		t.Fatalf("first run: result = %q, want %q", got, "x")
	}
	if len(first.Invocations) != 2 {
		t.Fatalf("first run: invocations = %d, want 2", len(first.Invocations))
	}

	// After Reset the next run must start a new execution: the step
	// function runs again and the handler sees the new input.
	runner.Reset()

	second := runner.RunUntilComplete(t, "y")
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second run: status = %s, want SUCCEEDED", second.Status)
	}
	// The step function ran again (executions is now 2) and the new
	// execution saw the new input, so the result is "yy".
	if got, _ := durabletest.ResultAs[string](second); got != "yy" {
		t.Errorf("second run: result = %q, want %q", got, "yy")
	}
	if executions.Load() != 2 {
		t.Errorf("step executions = %d, want 2", executions.Load())
	}

	// The second run's log is a fresh one: same operation count, fresh
	// events, and request IDs counting from 1 again.
	if len(second.Operations) != len(first.Operations) {
		t.Errorf("second run: %d operations, first run had %d", len(second.Operations), len(first.Operations))
	}
	if len(second.Events) != len(first.Events) {
		t.Errorf("second run: %d events, first run had %d", len(second.Events), len(first.Events))
	}
	if len(second.Invocations) != 2 {
		t.Fatalf("second run: invocations = %d, want 2", len(second.Invocations))
	}
	if second.Invocations[0].RequestID != first.Invocations[0].RequestID {
		t.Errorf("second run: first request ID = %q, want %q", second.Invocations[0].RequestID, first.Invocations[0].RequestID)
	}
	if got := second.EventTypes()[0]; got != "ExecutionStarted" {
		t.Errorf("second run: first event = %q, want ExecutionStarted", got)
	}
}

func TestResetClearsOpenCallbacksAndPreservesConfig(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		prefix, err := durable.Step(ctx, "prefix", func(_ durable.StepContext) (string, error) {
			return event + ":", nil
		})
		if err != nil {
			return "", err
		}
		cb, err := durable.CreateCallback[string](ctx, "cb")
		if err != nil {
			return "", err
		}
		v, err := cb.Result()
		return prefix + v, err
	}

	// A custom serdes proves that handler configuration survives a reset.
	serdes := &recordingSerdes{}
	runner := durabletest.NewLocalRunner(handler, durable.WithSerdes(serdes))

	result := runner.RunUntilComplete(t, "in")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	if len(runner.OpenCallbacks()) != 1 {
		t.Fatalf("open callbacks = %d, want 1", len(runner.OpenCallbacks()))
	}
	cbID := runner.OpenCallbacks()[0].CallbackID

	runner.Reset()

	if got := runner.OpenCallbacks(); len(got) != 0 {
		t.Fatalf("open callbacks after reset = %v, want none", got)
	}
	if err := runner.SendCallbackSuccess(cbID, "late"); err == nil {
		t.Error("SendCallbackSuccess on a callback from before the reset succeeded, want error")
	}

	// The handler still runs with the same serdes.
	before := serdes.calls.Load()
	result = runner.RunUntilComplete(t, "again")
	if result.Status != durabletest.Pending {
		t.Fatalf("status after reset = %s, want PENDING", result.Status)
	}
	if serdes.calls.Load() == before {
		t.Error("custom serdes was not used after reset")
	}
	if len(runner.OpenCallbacks()) != 1 {
		t.Fatalf("open callbacks after re-run = %d, want 1", len(runner.OpenCallbacks()))
	}
	if err := runner.SendCallbackSuccess(runner.OpenCallbacks()[0].CallbackID, "done"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}
	result = runner.RunUntilComplete(t, "again")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("final status = %s, want SUCCEEDED", result.Status)
	}
	if got, _ := durabletest.ResultAs[string](result); got != "again:done" {
		t.Errorf("final result = %q, want %q", got, "again:done")
	}
}

func TestResetPreservesRegisteredFunctions(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		return durable.Invoke[string](ctx, "call", "target-fn", event)
	}
	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction("target-fn", durabletest.PlainFunction(func(_ context.Context, in string) (string, error) {
		return "echo:" + in, nil
	}))

	result := runner.RunUntilComplete(t, "one")
	if got, _ := durabletest.ResultAs[string](result); got != "echo:one" {
		t.Fatalf("result = %q, want %q", got, "echo:one")
	}

	runner.Reset()

	result = runner.RunUntilComplete(t, "two")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status after reset = %s, want SUCCEEDED (registered function should still resolve)", result.Status)
	}
	if got, _ := durabletest.ResultAs[string](result); got != "echo:two" {
		t.Errorf("result after reset = %q, want %q", got, "echo:two")
	}
}

func TestResetClearsPendingInvokesAndOmitSchedule(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		return durable.Invoke[string](ctx, "call", "unregistered", event)
	}
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "in")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	// Schedule a token omission that has not fired yet.
	runner.OmitTokenOnCheckpoint(1)

	runner.Reset()

	// The pre-reset invoke is gone.
	if err := runner.CompleteChainedInvoke("call", "late"); err == nil {
		t.Error("CompleteChainedInvoke on an invoke from before the reset succeeded, want error")
	}

	// The omission schedule is gone: the re-run checkpoints normally and
	// blocks on the new invoke rather than ending early without a token.
	result = runner.RunUntilComplete(t, "in")
	if result.Status != durabletest.Pending {
		t.Fatalf("status after reset = %s, want PENDING", result.Status)
	}
	if err := runner.CompleteChainedInvoke("call", "ok"); err != nil {
		t.Fatalf("CompleteChainedInvoke after reset: %v", err)
	}
	result = runner.RunUntilComplete(t, "in")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("final status = %s, want SUCCEEDED", result.Status)
	}
}

// recordingSerdes is a JSON serdes that counts how often it is used.
type recordingSerdes struct {
	calls atomic.Int32
}

func (s *recordingSerdes) Marshal(ctx context.Context, meta durable.SerdesContext, v any) ([]byte, error) {
	s.calls.Add(1)
	return durable.JSONSerdes.Marshal(ctx, meta, v)
}

func (s *recordingSerdes) Unmarshal(ctx context.Context, meta durable.SerdesContext, data []byte, v any) error {
	s.calls.Add(1)
	return durable.JSONSerdes.Unmarshal(ctx, meta, data, v)
}

// --- OperationByNameAndIndex ---

func TestOperationByNameAndIndex(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		var out string
		for i := range 3 {
			s, err := durable.Step(ctx, "repeat", func(_ durable.StepContext) (string, error) {
				return string(rune('a' + i)), nil
			})
			if err != nil {
				return "", err
			}
			out += s
		}
		return out, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	for i, want := range []string{"a", "b", "c"} {
		op := result.OperationByNameAndIndex("repeat", i)
		if op == nil {
			t.Fatalf("OperationByNameAndIndex(repeat, %d) = nil", i)
		}
		got, err := durabletest.OperationResultAs[string](op)
		if err != nil {
			t.Fatalf("occurrence %d: %v", i, err)
		}
		if got != want {
			t.Errorf("occurrence %d result = %q, want %q", i, got, want)
		}
	}

	// Occurrence 0 is what Operation returns.
	if result.OperationByNameAndIndex("repeat", 0) != result.Operation("repeat") {
		t.Error("occurrence 0 differs from Operation(name)")
	}

	// Ordering follows Operations.
	var seen []string
	for _, op := range result.Operations {
		if op.Name == "repeat" {
			seen = append(seen, op.ID)
		}
	}
	for i, id := range seen {
		if got := result.OperationByNameAndIndex("repeat", i).ID; got != id {
			t.Errorf("occurrence %d ID = %q, want %q (Operations order)", i, got, id)
		}
	}

	// Out of range and unknown name return nil.
	if result.OperationByNameAndIndex("repeat", 3) != nil {
		t.Error("occurrence 3 should be nil")
	}
	if result.OperationByNameAndIndex("repeat", -1) != nil {
		t.Error("negative occurrence should be nil")
	}
	if result.OperationByNameAndIndex("missing", 0) != nil {
		t.Error("unknown name should be nil")
	}
}

// --- FormatTree / WriteTree ---

func TestFormatTreeNestedResult(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	result := &durabletest.TestResult{
		Status: durabletest.Succeeded,
		Operations: []durabletest.TestOperation{
			{ID: "op-1", Name: "validate", Type: "STEP", SubType: "Step", Status: "SUCCEEDED",
				StartTime: start, EndTime: start.Add(1500 * time.Millisecond)},
			{ID: "op-2", Name: "fulfil", Type: "CONTEXT", SubType: "RunInChildContext", Status: "SUCCEEDED",
				StartTime: start.Add(2 * time.Second), EndTime: start.Add(5 * time.Second)},
			{ID: "op-3", ParentID: "op-2", Name: "label", Type: "CHAINED_INVOKE", SubType: "Invoke", Status: "SUCCEEDED"},
			{ID: "op-4", ParentID: "op-2", Name: "pick", Type: "CONTEXT", SubType: "RunInChildContext", Status: "FAILED"},
			{ID: "op-5", ParentID: "op-4", Name: "", Type: "STEP", SubType: "Step", Status: "FAILED",
				StartTime: start.Add(3 * time.Second)},
			{ID: "op-6", ParentID: "gone", Name: "orphan", Type: "WAIT", Status: "STARTED"},
			{ID: "op-7", Name: "notify", Type: "CALLBACK", SubType: "CreateCallback", Status: "STARTED"},
		},
	}

	want := strings.Join([]string{
		"NAME      TYPE            SUBTYPE            STATUS     START                     END                       DURATION",
		"validate  STEP            Step               SUCCEEDED  2026-01-02T03:04:05.000Z  2026-01-02T03:04:06.500Z  1.5s",
		"fulfil    CONTEXT         RunInChildContext  SUCCEEDED  2026-01-02T03:04:07.000Z  2026-01-02T03:04:10.000Z  3s",
		"  label   CHAINED_INVOKE  Invoke             SUCCEEDED  -                         -                         -",
		"  pick    CONTEXT         RunInChildContext  FAILED     -                         -                         -",
		"    -     STEP            Step               FAILED     2026-01-02T03:04:08.000Z  -                         -",
		"orphan    WAIT            -                  STARTED    -                         -                         -",
		"notify    CALLBACK        CreateCallback     STARTED    -                         -                         -",
		"",
	}, "\n")

	got := result.FormatTree()
	if got != want {
		t.Errorf("FormatTree() mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}

	// Deterministic: the same result renders the same text.
	if again := result.FormatTree(); again != got {
		t.Error("FormatTree() is not deterministic")
	}

	// WriteTree renders the same text to a writer.
	var buf bytes.Buffer
	if err := result.WriteTree(&buf); err != nil {
		t.Fatalf("WriteTree: %v", err)
	}
	if buf.String() != want {
		t.Errorf("WriteTree() differs from FormatTree()\n%s", buf.String())
	}
}

func TestFormatTreeColumnSelection(t *testing.T) {
	result := &durabletest.TestResult{
		Operations: []durabletest.TestOperation{
			{ID: "a", Name: "outer", Type: "CONTEXT", Status: "SUCCEEDED"},
			{ID: "b", ParentID: "a", Name: "inner", Type: "STEP", Status: "SUCCEEDED"},
		},
	}

	want := strings.Join([]string{
		"ID  PARENT  STATUS     NAME",
		"a   -       SUCCEEDED  outer",
		"b   a       SUCCEEDED    inner",
		"",
	}, "\n")
	got := result.FormatTree(durabletest.ColumnID, durabletest.ColumnParentID, durabletest.ColumnStatus, durabletest.ColumnName)
	if got != want {
		t.Errorf("FormatTree(columns) mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}

	// Timestamps are rendered in UTC regardless of the value's location.
	loc := time.FixedZone("plus2", 2*60*60)
	ts := &durabletest.TestResult{Operations: []durabletest.TestOperation{
		{Name: "s", StartTime: time.Date(2026, 1, 2, 5, 0, 0, 0, loc)},
	}}
	out := ts.FormatTree(durabletest.ColumnStartTime)
	if !strings.Contains(out, "2026-01-02T03:00:00.000Z") {
		t.Errorf("start time not rendered in UTC:\n%s", out)
	}
}

func TestFormatTreeEmptyAndErrors(t *testing.T) {
	empty := &durabletest.TestResult{}
	if got := empty.FormatTree(); got != "(no operations)\n" {
		t.Errorf("empty FormatTree() = %q", got)
	}
	var nilResult *durabletest.TestResult
	if got := nilResult.FormatTree(); got != "(no operations)\n" {
		t.Errorf("nil FormatTree() = %q", got)
	}

	// Every operation is rendered even when the parent links form a cycle:
	// the first operation of the cycle is placed at the top level and the
	// rest hang under it.
	cyclic := &durabletest.TestResult{Operations: []durabletest.TestOperation{
		{ID: "x", ParentID: "y", Name: "x"},
		{ID: "y", ParentID: "x", Name: "y"},
	}}
	if got, want := cyclic.FormatTree(durabletest.ColumnName), "NAME\nx\n  y\n"; got != want {
		t.Errorf("cyclic parents: got %q, want %q", got, want)
	}

	// Writer errors are returned.
	failing := &failingWriter{err: errors.New("disk full")}
	if err := cyclic.WriteTree(failing); !errors.Is(err, failing.err) {
		t.Errorf("WriteTree error = %v, want %v", err, failing.err)
	}
}

type failingWriter struct{ err error }

func (w *failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestFormatTreeLocalNestedRun(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		return durable.RunInChildContext(ctx, "outer", func(c durable.Context) (string, error) {
			a, err := durable.Step(c, "inner-a", func(_ durable.StepContext) (string, error) { return "a", nil })
			if err != nil {
				return "", err
			}
			return durable.RunInChildContext(c, "middle", func(c2 durable.Context) (string, error) {
				b, err := durable.Step(c2, "inner-b", func(_ durable.StepContext) (string, error) { return "b", nil })
				return a + b, err
			})
		})
	}

	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	want := strings.Join([]string{
		"NAME         STATUS",
		"outer        SUCCEEDED",
		"  inner-a    SUCCEEDED",
		"  middle     SUCCEEDED",
		"    inner-b  SUCCEEDED",
		"",
	}, "\n")
	got := result.FormatTree(durabletest.ColumnName, durabletest.ColumnStatus)
	if got != want {
		t.Errorf("FormatTree() of local nested run mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}

func TestFormatTreeLocalRunTimings(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		return durable.RunInChildContext(ctx, "outer", func(c durable.Context) (string, error) {
			s, err := durable.Step(c, "work", func(_ durable.StepContext) (string, error) {
				time.Sleep(2 * time.Millisecond)
				return event, nil
			})
			if err != nil {
				return "", err
			}
			if err := durable.Wait(c, "pause", time.Minute); err != nil {
				return "", err
			}
			cb, err := durable.CreateCallback[string](c, "approve")
			if err != nil {
				return "", err
			}
			v, err := cb.Result()
			return s + v, err
		})
	}

	before := time.Now()
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING (blocked on callback)", result.Status)
	}
	cbs := runner.OpenCallbacks()
	if len(cbs) != 1 {
		t.Fatalf("open callbacks = %d, want 1", len(cbs))
	}
	if err := runner.SendCallbackSuccess(cbs[0].CallbackID, "!"); err != nil {
		t.Fatalf("SendCallbackSuccess: %v", err)
	}
	result = runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	after := time.Now()

	// Every operation settled, so every row has a start, an end, and a
	// non-negative duration within the test's own wall-clock window.
	for _, name := range []string{"outer", "work", "pause", "approve"} {
		op := result.Operation(name)
		if op == nil {
			t.Fatalf("operation %q missing:\n%s", name, result.FormatTree())
		}
		if op.StartTime.IsZero() || op.EndTime.IsZero() {
			t.Errorf("%s: StartTime=%v EndTime=%v, want both set", name, op.StartTime, op.EndTime)
			continue
		}
		if op.StartTime.Before(before) || op.EndTime.After(after) {
			t.Errorf("%s: [%v, %v] outside test window [%v, %v]", name, op.StartTime, op.EndTime, before, after)
		}
		if op.EndTime.Before(op.StartTime) {
			t.Errorf("%s: EndTime %v precedes StartTime %v", name, op.EndTime, op.StartTime)
		}
	}
	// The step's start precedes its end by at least the time it slept.
	if work := result.Operation("work"); work.EndTime.Sub(work.StartTime) < 2*time.Millisecond {
		t.Errorf("work duration = %v, want at least 2ms", work.EndTime.Sub(work.StartTime))
	}
	// The wait and the callback settled after the step, in program order.
	if pause := result.Operation("pause"); pause.StartTime.Before(result.Operation("work").EndTime) {
		t.Errorf("pause started at %v, before work ended at %v", pause.StartTime, result.Operation("work").EndTime)
	}

	// The operation timestamps are the same instants the history events
	// carry, so the tree and the event log tell one story.
	for _, ev := range result.Events {
		if ev.EventType != "StepSucceeded" || ev.Name == nil || *ev.Name != "work" {
			continue
		}
		if got := *ev.EventTimestamp; !got.Equal(result.Operation("work").EndTime) {
			t.Errorf("StepSucceeded event at %v, operation EndTime %v", got, result.Operation("work").EndTime)
		}
	}

	// The rendered timing columns hold values, not placeholders.
	out := result.FormatTree(durabletest.ColumnName, durabletest.ColumnStartTime, durabletest.ColumnEndTime, durabletest.ColumnDuration)
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("FormatTree() rendered %d lines, want header + 4 rows:\n%s", len(lines), out)
	}
	for _, line := range lines[1:] {
		cells := strings.Fields(line)
		if len(cells) != 4 {
			t.Fatalf("row %q has %d cells, want 4", line, len(cells))
		}
		for i, cell := range cells[1:] {
			if cell == "-" {
				t.Errorf("row %q: column %d is a placeholder, want a value", line, i+1)
			}
		}
		if _, err := time.Parse("2006-01-02T15:04:05.000Z", cells[1]); err != nil {
			t.Errorf("row %q: start %q is not a timestamp: %v", line, cells[1], err)
		}
		if _, err := time.ParseDuration(cells[3]); err != nil {
			t.Errorf("row %q: duration %q does not parse: %v", line, cells[3], err)
		}
	}
}

func TestFormatTreeLocalRunUnsettledOperationHasNoEnd(t *testing.T) {
	handler := func(ctx durable.Context, event string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "open")
		if err != nil {
			return "", err
		}
		return cb.Result()
	}
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, "go")
	if result.Status != durabletest.Pending {
		t.Fatalf("status = %s, want PENDING", result.Status)
	}
	op := result.Operation("open")
	if op == nil || op.StartTime.IsZero() {
		t.Fatalf("open callback should have a start time: %+v", op)
	}
	if !op.EndTime.IsZero() {
		t.Errorf("open callback has EndTime %v, want zero while still STARTED", op.EndTime)
	}
	out := result.FormatTree(durabletest.ColumnName, durabletest.ColumnEndTime, durabletest.ColumnDuration)
	if !strings.HasSuffix(strings.TrimSpace(out), "open  -    -") {
		t.Errorf("unsettled operation should render END and DURATION as \"-\":\n%s", out)
	}
}

func TestDefaultTreeColumnsReturnsCopy(t *testing.T) {
	want := []durabletest.TreeColumn{
		durabletest.ColumnName, durabletest.ColumnType, durabletest.ColumnSubType, durabletest.ColumnStatus,
		durabletest.ColumnStartTime, durabletest.ColumnEndTime, durabletest.ColumnDuration,
	}
	got := durabletest.DefaultTreeColumns()
	if len(got) != len(want) {
		t.Fatalf("DefaultTreeColumns() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("DefaultTreeColumns()[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// Mutating the returned slice changes neither later calls nor the
	// default rendering.
	result := &durabletest.TestResult{Operations: []durabletest.TestOperation{{ID: "a", Name: "n", Type: "STEP", Status: "SUCCEEDED"}}}
	before := result.FormatTree()
	got[0] = durabletest.ColumnID
	got = append(got, durabletest.ColumnParentID)
	if len(got) != len(want)+1 {
		t.Fatalf("appending to the returned slice gave %d columns, want %d", len(got), len(want)+1)
	}
	if again := durabletest.DefaultTreeColumns(); again[0] != durabletest.ColumnName || len(again) != len(want) {
		t.Errorf("DefaultTreeColumns() changed after caller mutation: %v", again)
	}
	if after := result.FormatTree(); after != before {
		t.Errorf("FormatTree() default output changed after caller mutated DefaultTreeColumns():\n%s", after)
	}
	// The default rendering equals rendering with the default set spelled out.
	if explicit := result.FormatTree(durabletest.DefaultTreeColumns()...); explicit != before {
		t.Errorf("FormatTree(DefaultTreeColumns()...) differs from FormatTree()")
	}
}
