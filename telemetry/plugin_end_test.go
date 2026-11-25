package telemetry

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func TestPlugin_LateHookDoesNotEmitAfterInvocationEnd(t *testing.T) {
	for _, status := range []durable.PluginInvocationStatus{
		durable.PluginInvocationSucceeded,
		durable.PluginInvocationFailed,
		durable.PluginInvocationPending,
		durable.PluginInvocationRetrying,
	} {
		t.Run(string(status), func(t *testing.T) {
			exp := &capturingExporter{}
			p := &telemetryInvocation{
				env:    New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}}),
				record: &Record{Status: StatusRunning, ExecutionArn: "test-execution"},
				byOpID: make(map[string]int), resultText: make(map[string]string), endReported: make(map[string]struct{}), sampledIn: true,
			}
			ctx := context.Background()
			p.onInvocationEnd(ctx, durable.InvocationEndHookInfo{Status: status})
			if n := len(exp.snapshot()); n != 1 {
				t.Fatalf("final record missing: %d", n)
			}
			p.onOperationStart(ctx, durable.OperationHookInfo{ID: "late-op", Type: "STEP", Status: durable.PluginOperationStarted})
			p.onChangeWG.Wait()
			records := exp.snapshot()
			if len(records) != 1 {
				t.Fatalf("late hook emitted after final: got %d records", len(records))
			}
		})
	}
}

func TestPlugin_ConcurrentHooksAndInvocationEnd(t *testing.T) {
	for i := 0; i < 50; i++ {
		exp := &capturingExporter{}
		p := &telemetryInvocation{
			env:    New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}}),
			record: &Record{Status: StatusRunning, ExecutionArn: "test-execution"},
			byOpID: make(map[string]int), resultText: make(map[string]string), endReported: make(map[string]struct{}), sampledIn: true,
		}
		ctx := context.Background()
		start := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-start
			for j := 0; j < 100; j++ {
				p.onOperationStart(ctx, durable.OperationHookInfo{ID: "op", Type: "STEP", Status: durable.PluginOperationStarted})
			}
		}()
		close(start)
		p.onInvocationEnd(ctx, durable.InvocationEndHookInfo{Status: durable.PluginInvocationSucceeded})
		atEnd := len(exp.snapshot())
		<-done
		p.onChangeWG.Wait()
		records := exp.snapshot()
		if len(records) != atEnd {
			t.Fatalf("records emitted after invocation end: %d -> %d", atEnd, len(records))
		}
		if records[len(records)-1].Status != StatusSucceeded {
			t.Fatalf("last record is not terminal: %s", records[len(records)-1].Status)
		}
	}
}

// TestPlugin_OnOperationChange_SkippedChangeEmitsNothing asserts that in
// EmitOnChange mode a change which records nothing emits no record. The
// step's end is reported first, so the later change for it is skipped.
func TestPlugin_OnOperationChange_SkippedChangeEmitsNothing(t *testing.T) {
	exp := &capturingExporter{}
	p := &telemetryInvocation{
		env:    New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}}),
		record: &Record{Status: StatusRunning, ExecutionArn: "test-execution"},
		byOpID: make(map[string]int), resultText: make(map[string]string), endReported: make(map[string]struct{}), sampledIn: true,
	}
	ctx := context.Background()
	step := durable.OperationHookInfo{ID: "op-1", Name: "step", Type: "STEP", Status: durable.PluginOperationSucceeded}
	p.onOperationEnd(ctx, step)
	p.onChangeWG.Wait()
	before := len(exp.snapshot())

	started := step
	started.Status = durable.PluginOperationStarted
	p.onOperationChange(ctx, durable.OperationChangeHookInfo{
		UpdatedOperations: map[string]durable.OperationHookInfo{"op-1": started},
		Operations:        map[string]durable.OperationHookInfo{"op-1": started},
	})
	p.onChangeWG.Wait()
	if after := len(exp.snapshot()); after != before {
		t.Errorf("the skipped change emitted %d records, want 0", after-before)
	}
}

// TestPlugin_OnOperationChange_AppendsOldestFirst asserts that one change
// with several new operations appends them by start time. Truncate removes
// operations from the front of the slice as the oldest.
func TestPlugin_OnOperationChange_AppendsOldestFirst(t *testing.T) {
	p := &telemetryInvocation{
		env:    New(Config{}),
		record: &Record{Status: StatusRunning, ExecutionArn: "test-execution"},
		byOpID: make(map[string]int), resultText: make(map[string]string), endReported: make(map[string]struct{}), sampledIn: true,
	}
	base := time.Unix(1700000000, 0)
	updated := make(map[string]durable.OperationHookInfo)
	var want []string
	for i := range 16 {
		// IDs sort in the reverse of the start times.
		id := fmt.Sprintf("op-%02d", 15-i)
		updated[id] = durable.OperationHookInfo{ID: id, Type: "STEP", Status: durable.PluginOperationStarted, StartTimestamp: base.Add(time.Duration(i) * time.Second)}
		want = append(want, id)
	}
	p.onOperationChange(context.Background(), durable.OperationChangeHookInfo{UpdatedOperations: updated, Operations: updated})

	var got []string
	for _, op := range p.record.Operations {
		got = append(got, op.ID)
	}
	if !slices.Equal(got, want) {
		t.Errorf("operation order = %v, want oldest first %v", got, want)
	}
}
