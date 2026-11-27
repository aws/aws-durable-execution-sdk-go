package telemetry

import (
	"context"
	"errors"
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

// TestResume_LocalRunnerReplayedOperationsKeepRecordContents runs a handler
// over three invocations through the local runner. Each wait suspends one
// invocation. The third invocation replays the first step, the first wait,
// and the second step. The telemetry record reads them from the operations
// of OnInvocationEnd, like every other operation. The final record
// must hold every operation once, with its type, its terminal status, and
// its result. The local runner's invocation payload carries no operation
// timestamps. So the operations in the final record have none, and the
// test does not check them.
func TestResume_LocalRunnerReplayedOperationsKeepRecordContents(t *testing.T) {
	handler := func(ctx durable.Context, _ string) (string, error) {
		if _, err := durable.Step(ctx, "s1", func(durable.StepContext) (string, error) { return "one", nil }); err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "w1", 5*time.Second); err != nil {
			return "", err
		}
		if _, err := durable.Step(ctx, "s2", func(durable.StepContext) (string, error) { return "two", nil }); err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "w2", 5*time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	exp := &capturingExporter{}
	factory := New(Config{
		EmitMode:        EmitAlways,
		OperationDetail: OperationDetailFullTree,
		Exporters:       []Exporter{exp},
		Content:         withResults("s1", "s2"),
	})
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(factory))

	for i, want := range []durabletest.ExecutionStatus{durabletest.Pending, durabletest.Pending, durabletest.Succeeded} {
		if i > 0 && !runner.CompletePendingTimers() {
			t.Fatalf("invocation %d: CompletePendingTimers completed no timer", i+1)
		}
		res, err := runner.Run("event")
		if err != nil {
			t.Fatalf("invocation %d Run: %v", i+1, err)
		}
		if res.Status != want {
			t.Fatalf("invocation %d status = %s, want %s", i+1, res.Status, want)
		}
	}

	records := exp.snapshot()
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	final := records[2]
	if final.Status != StatusSucceeded {
		t.Fatalf("final status = %s, want SUCCEEDED", final.Status)
	}
	if len(final.Operations) != 4 {
		t.Fatalf("final record has %d operations, want 4: %+v", len(final.Operations), final.Operations)
	}
	wantResult := map[string]string{"s1": `"one"`, "s2": `"two"`}
	names := map[string]int{}
	for _, op := range final.Operations {
		names[op.Name]++
		if op.Status != string(durable.PluginOperationSucceeded) {
			t.Errorf("operation %q status = %s, want SUCCEEDED", op.Name, op.Status)
		}
		if wantType := map[byte]string{'s': "STEP", 'w': "WAIT"}[op.Name[0]]; op.Type != wantType {
			t.Errorf("operation %q type = %s, want %s", op.Name, op.Type, wantType)
		}
		if want, ok := wantResult[op.Name]; ok && string(op.Result) != want {
			t.Errorf("operation %q result = %s, want %s", op.Name, op.Result, want)
		}
	}
	for _, name := range []string{"s1", "w1", "s2", "w2"} {
		if names[name] != 1 {
			t.Errorf("final record has operation %q %d times, want 1 (%v)", name, names[name], names)
		}
	}
}

// TestResume_LocalRunnerReplayedOperationsKeepAttempt runs a handler to
// completion through the local runner. A step fails twice and then
// succeeds. A wait-for-condition stops on its third check. Then a wait
// suspends the invocation. The last invocation replays the step and the
// wait-for-condition from terminal checkpoints. The telemetry record reads
// them from the operations of OnInvocationEnd, like every other operation.
// The final record must report, for both, the attempt count that the
// checkpoint holds. The test reads that count from the runner's result, so
// it does not depend on how the runner numbers attempts.
func TestResume_LocalRunnerReplayedOperationsKeepAttempt(t *testing.T) {
	var stepRuns atomic.Int32
	handler := func(ctx durable.Context, _ string) (string, error) {
		_, err := durable.Step(ctx, "s1", func(durable.StepContext) (string, error) {
			if stepRuns.Add(1) <= 2 {
				return "", errors.New("transient failure")
			}
			return "one", nil
		})
		if err != nil {
			return "", err
		}
		_, err = durable.WaitForCondition(ctx, "c1", func(_ durable.StepContext, checks int) (int, error) {
			return checks + 1, nil
		}, durable.ConditionConfig[int]{
			WaitStrategy: func(checks int, _ int) durable.WaitDecision {
				return durable.WaitDecision{Continue: checks < 3, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "w1", 5*time.Second); err != nil {
			return "", err
		}
		return "done", nil
	}

	// The spy shows that the last invocation started after both
	// operations were terminal, so that invocation replays them.
	spy := &resumeSpy{starts: make(chan durable.InvocationHookInfo, 16)}
	exp := &capturingExporter{}
	factory := New(Config{
		EmitMode:        EmitAlways,
		OperationDetail: OperationDetailFullTree,
		Exporters:       []Exporter{exp},
	})
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(spy, factory))

	res, err := runner.RunUntilComplete("event")
	if err != nil {
		t.Fatalf("RunUntilComplete: %v", err)
	}
	if res.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", res.Status)
	}

	close(spy.starts)
	var last durable.InvocationHookInfo
	for info := range spy.starts {
		last = info
	}
	for _, op := range last.Operations {
		if (op.Name == "s1" || op.Name == "c1") && op.Status != durable.PluginOperationSucceeded {
			t.Fatalf("last invocation started with %q in status %s, want SUCCEEDED", op.Name, op.Status)
		}
	}

	records := exp.snapshot()
	if len(records) == 0 {
		t.Fatal("no record was exported")
	}
	final := records[len(records)-1]
	if final.Status != StatusSucceeded {
		t.Fatalf("final status = %s, want SUCCEEDED", final.Status)
	}
	checkpointed := map[string]int{}
	for _, op := range res.Operations {
		if op.StepDetails != nil {
			checkpointed[op.Name] = int(op.StepDetails.Attempt)
		}
	}
	recorded := map[string]int{}
	for _, op := range final.Operations {
		recorded[op.Name] = op.Attempt
	}
	for _, name := range []string{"s1", "c1"} {
		want := checkpointed[name]
		if want == 0 {
			t.Fatalf("operation %q has no checkpointed attempt count; the test needs one", name)
		}
		if got := recorded[name]; got != want {
			t.Errorf("operation %q attempt = %d, want the checkpointed %d", name, got, want)
		}
	}
}

// TestSampledOut_LocalRunnerExecutionCompletes runs a handler through the
// local runner with a factory that samples every execution out. The plugin
// of such an invocation registers only EnrichLogContext. The execution must
// still suspend on the wait, resume, and succeed, and no record is emitted.
func TestSampledOut_LocalRunnerExecutionCompletes(t *testing.T) {
	handler := func(ctx durable.Context, _ string) (string, error) {
		one, err := durable.Step(ctx, "s1", func(durable.StepContext) (string, error) { return "one", nil })
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "w1", 5*time.Second); err != nil {
			return "", err
		}
		return one, nil
	}

	exp := &capturingExporter{}
	// At this rate the sampling threshold is 0 buckets, so every ARN
	// samples out.
	factory := New(Config{EmitMode: EmitOnChange, SamplingRate: 1e-9, Exporters: []Exporter{exp}})
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(factory))

	for i, want := range []durabletest.ExecutionStatus{durabletest.Pending, durabletest.Succeeded} {
		if i > 0 && !runner.CompletePendingTimers() {
			t.Fatalf("invocation %d: CompletePendingTimers completed no timer", i+1)
		}
		res, err := runner.Run("event")
		if err != nil {
			t.Fatalf("invocation %d Run: %v", i+1, err)
		}
		if res.Status != want {
			t.Fatalf("invocation %d status = %s, want %s", i+1, res.Status, want)
		}
	}
	if records := exp.snapshot(); len(records) != 0 {
		t.Errorf("got %d records for a sampled-out execution, want 0", len(records))
	}
}
