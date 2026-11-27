package telemetry

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// newTestInvocation returns the state of one invocation of a plugin in the
// EmitOnChange mode that exports to exp. EmitOnChange is the mode in which
// every hook builds the operation list and schedules an emission.
func newTestInvocation(exp Exporter, sampledIn bool) *telemetryInvocation {
	return &telemetryInvocation{
		env:       New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}}),
		record:    &Record{Status: StatusRunning, ExecutionArn: "test-execution"},
		results:   make(map[string]recordedResult),
		sampledIn: sampledIn,
	}
}

func TestPlugin_LateHookDoesNotEmitAfterInvocationEnd(t *testing.T) {
	for _, status := range []durable.PluginInvocationStatus{
		durable.PluginInvocationSucceeded,
		durable.PluginInvocationFailed,
		durable.PluginInvocationPending,
		durable.PluginInvocationRetrying,
	} {
		t.Run(string(status), func(t *testing.T) {
			exp := &capturingExporter{}
			p := newTestInvocation(exp, true)
			ctx := context.Background()
			p.onInvocationEnd(ctx, durable.InvocationEndHookInfo{Status: status})
			if n := len(exp.snapshot()); n != 1 {
				t.Fatalf("final record missing: %d", n)
			}
			late := opsByID(durable.OperationHookInfo{ID: "late-op", Type: "STEP", Status: durable.PluginOperationStarted})
			p.onOperationChange(ctx, durable.OperationChangeHookInfo{UpdatedOperations: late, Operations: late})
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
		p := newTestInvocation(exp, true)
		ctx := context.Background()
		start := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-start
			change := opsByID(durable.OperationHookInfo{ID: "op", Type: "STEP", Status: durable.PluginOperationStarted})
			for j := 0; j < 100; j++ {
				p.onOperationChange(ctx, durable.OperationChangeHookInfo{UpdatedOperations: change, Operations: change})
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

// TestPlugin_OperationsOrderedOldestFirst asserts that the record lists
// the operations of a snapshot by start time. Truncate removes operations
// from the front of the slice as the oldest.
func TestPlugin_OperationsOrderedOldestFirst(t *testing.T) {
	exp := &capturingExporter{}
	p := newTestInvocation(exp, true)
	base := time.Unix(1700000000, 0)
	ops := make(map[string]durable.OperationHookInfo)
	var want []string
	for i := range 16 {
		// IDs sort in the reverse of the start times.
		id := fmt.Sprintf("op-%02d", 15-i)
		ops[id] = durable.OperationHookInfo{ID: id, Type: "STEP", Status: durable.PluginOperationStarted, StartTimestamp: base.Add(time.Duration(i) * time.Second)}
		want = append(want, id)
	}
	p.onInvocationEnd(context.Background(), durable.InvocationEndHookInfo{Status: durable.PluginInvocationPending, Operations: ops})

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	var got []string
	for _, op := range records[0].Operations {
		got = append(got, op.ID)
	}
	if !slices.Equal(got, want) {
		t.Errorf("operation order = %v, want oldest first %v", got, want)
	}
}

// TestPlugin_SampledOutBuildsNoOperationList asserts that a sampled-out
// invocation builds no operation list in any hook. Its record is never
// emitted, so the list would be discarded.
func TestPlugin_SampledOutBuildsNoOperationList(t *testing.T) {
	exp := &capturingExporter{}
	p := newTestInvocation(exp, false)
	ctx := context.Background()
	ops := opsByID(durable.OperationHookInfo{ID: "op-1", Name: "step", Type: "STEP", Status: durable.PluginOperationSucceeded})

	p.onInvocationStart(ctx, durable.InvocationHookInfo{Operations: ops})
	p.onOperationChange(ctx, durable.OperationChangeHookInfo{UpdatedOperations: ops, Operations: ops})
	p.onInvocationEnd(ctx, durable.InvocationEndHookInfo{Status: durable.PluginInvocationSucceeded, Operations: ops})

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.record.Operations) != 0 {
		t.Errorf("sampled-out invocation built an operation list: %+v", p.record.Operations)
	}
	if len(p.results) != 0 {
		t.Errorf("sampled-out invocation stored results: %v", p.results)
	}
	if n := len(exp.snapshot()); n != 0 {
		t.Errorf("sampled-out invocation emitted %d records, want 0", n)
	}
}
