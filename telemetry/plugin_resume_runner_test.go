package telemetry

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// resumeSpy records, per invocation, the start snapshot maps that the
// handler passes to the plugin hooks.
type resumeSpy struct {
	starts chan durable.InvocationHookInfo
}

func (s *resumeSpy) CreatePlugin(context.Context, durable.InvocationHookInfo) (*durable.Plugin, error) {
	return &durable.Plugin{
		OnInvocationStart: func(_ context.Context, info durable.InvocationHookInfo) { s.starts <- info },
	}, nil
}

// TestResume_LocalRunnerCompletedChildContext runs a handler through the
// local runner. A child context completes with a nested step, and then a
// wait suspends the first invocation. After the timer completes, the
// second invocation replays the child context without running its body.
// The final full-tree record must still contain the nested step.
func TestResume_LocalRunnerCompletedChildContext(t *testing.T) {
	var childRuns, stepRuns atomic.Int32
	handler := func(ctx durable.Context, _ string) (string, error) {
		out, err := durable.RunInChildContext(ctx, "child", func(cc durable.Context) (string, error) {
			childRuns.Add(1)
			return durable.Step(cc, "nested", func(durable.StepContext) (string, error) {
				stepRuns.Add(1)
				return "nested-result", nil
			})
		})
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "pause", 5*time.Second); err != nil {
			return "", err
		}
		return out, nil
	}

	exp := &capturingExporter{}
	factory := New(Config{
		EmitMode:        EmitAlways,
		OperationDetail: OperationDetailFullTree,
		Exporters:       []Exporter{exp},
		Content:         withResults("nested"),
	})
	spy := &resumeSpy{starts: make(chan durable.InvocationHookInfo, 4)}
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(spy, factory))

	first, err := runner.Run("event")
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if first.Status != durabletest.Pending {
		t.Fatalf("first status = %s, want PENDING", first.Status)
	}
	if !runner.CompletePendingTimers() {
		t.Fatal("CompletePendingTimers completed no timer")
	}
	second, err := runner.Run("event")
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if second.Status != durabletest.Succeeded {
		t.Fatalf("second status = %s, want SUCCEEDED", second.Status)
	}

	if got := childRuns.Load(); got != 1 {
		t.Errorf("child body ran %d times, want 1", got)
	}
	if got := stepRuns.Load(); got != 1 {
		t.Errorf("nested step ran %d times, want 1", got)
	}

	// The spy sees the maps the factory received on each invocation.
	<-spy.starts
	resumeInfo := <-spy.starts
	if resumeInfo.IsFirstInvocation {
		t.Error("second invocation reports IsFirstInvocation")
	}
	idByName := map[string]string{}
	rootID := ""
	for id, op := range resumeInfo.Operations {
		idByName[op.Name] = id
		if op.Type == string(durable.OperationTypeExecution) {
			rootID = id
		}
	}
	if rootID == "" {
		t.Fatalf("Operations has no root execution operation: %+v", resumeInfo.Operations)
	}
	if idByName["child"] == "" || idByName["nested"] == "" || idByName["pause"] == "" {
		t.Fatalf("Operations lacks child, nested, or pause: %+v", resumeInfo.Operations)
	}
	if _, ok := resumeInfo.UpdatedOperations[idByName["nested"]]; ok {
		t.Error("UpdatedOperations contains the unchanged nested step")
	}
	if _, ok := resumeInfo.UpdatedOperations[idByName["child"]]; ok {
		t.Error("UpdatedOperations contains the unchanged child context")
	}
	if _, ok := resumeInfo.UpdatedOperations[idByName["pause"]]; !ok {
		t.Error("UpdatedOperations lacks the completed wait")
	}

	records := exp.snapshot()
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Status != StatusRunning || records[0].EndTimestamp != nil {
		t.Errorf("first record = status %s end %v, want RUNNING with no end", records[0].Status, records[0].EndTimestamp)
	}
	final := records[1]
	if final.Status != StatusSucceeded {
		t.Fatalf("final status = %s, want SUCCEEDED", final.Status)
	}
	nameCounts := map[string]int{}
	for _, op := range final.Operations {
		nameCounts[op.Name]++
	}
	for _, name := range []string{"child", "nested", "pause"} {
		if nameCounts[name] != 1 {
			t.Errorf("final record has operation %q %d times, want 1 (%v)", name, nameCounts[name], nameCounts)
		}
	}
	if counts := resumeOperationIDs(final); counts[rootID] != 0 {
		t.Errorf("final record contains the root execution operation")
	}
	if len(final.Operations) != 3 {
		t.Errorf("final record has %d operations, want 3: %+v", len(final.Operations), final.Operations)
	}
	for _, op := range final.Operations {
		if op.Name == "nested" && (op.ParentID != idByName["child"] || op.Result == nil) {
			t.Errorf("nested operation = %+v, want parent %q and a result", op, idByName["child"])
		}
	}
	if final.StartTimestamp == nil || records[0].StartTimestamp == nil || !final.StartTimestamp.Equal(*records[0].StartTimestamp) {
		t.Errorf("start timestamps differ across invocations: first %v final %v", records[0].StartTimestamp, final.StartTimestamp)
	}
}
