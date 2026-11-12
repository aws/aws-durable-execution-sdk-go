package telemetry

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type dispatchPhaseKey struct{}

// gatedExporter blocks only the first on-change export of each invocation.
// The gates are outside mu, so one invocation cannot block another's export.
type gatedExporter struct {
	mu      sync.Mutex
	gates   map[string]*exportGate
	events  map[string][]string
	records map[string][]Record
}

type exportGate struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (e *gatedExporter) Export(ctx context.Context, record Record) error {
	phase, _ := ctx.Value(dispatchPhaseKey{}).(string)
	arn := record.ExecutionArn
	e.mu.Lock()
	e.events[arn] = append(e.events[arn], phase+":start")
	e.mu.Unlock()
	if phase == "change" {
		if gate := e.gates[arn]; gate != nil {
			gate.once.Do(func() {
				close(gate.entered)
				<-gate.release
			})
		}
	}
	e.mu.Lock()
	e.events[arn] = append(e.events[arn], phase+":finish")
	e.records[arn] = append(e.records[arn], record)
	e.mu.Unlock()
	return nil
}

func TestPlugin_EmitOnChange_InvocationDispatchIsolation(t *testing.T) {
	for _, outcome := range []durable.PluginInvocationStatus{durable.PluginInvocationSucceeded, durable.PluginInvocationPending, durable.PluginInvocationRetrying} {
		t.Run(string(outcome), func(t *testing.T) {
			ctx := t.Context()
			changeCtx := context.WithValue(ctx, dispatchPhaseKey{}, "change")
			endCtx := context.WithValue(ctx, dispatchPhaseKey{}, "end")
			arns := []string{
				"arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/a/id-a",
				"arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/b/id-b",
			}
			exp := &gatedExporter{gates: make(map[string]*exportGate), events: make(map[string][]string), records: make(map[string][]Record)}
			for _, arn := range arns {
				gate := &exportGate{entered: make(chan struct{}), release: make(chan struct{})}
				exp.gates[arn] = gate
				t.Cleanup(func() {
					select {
					case <-gate.release:
					default:
						close(gate.release)
					}
				})
			}
			factory := New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}, Content: allContent()})
			done := []chan struct{}{make(chan struct{}), make(chan struct{})}
			for i, arn := range arns {
				plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{ExecutionArn: arn})
				plugin.OnOperationStart(changeCtx, durable.OperationHookInfo{ID: "same-id", Type: "STEP", Status: durable.PluginOperationStarted})
				awaitSignal(t, exp.gates[arn].entered)
				// The blocked dispatch must send this later update before the end emission.
				plugin.OnOperationEnd(changeCtx, durable.OperationHookInfo{ID: "same-id", Type: "STEP", Status: durable.PluginOperationSucceeded, Result: fmt.Sprintf("result-%d", i)})
				go func() {
					defer close(done[i])
					plugin.OnInvocationEnd(endCtx, durable.InvocationEndHookInfo{ExecutionArn: arn, Status: outcome})
				}()
			}
			// B must finish while A's exporter is still blocked.
			close(exp.gates[arns[1]].release)
			awaitSignal(t, done[1])
			exp.mu.Lock()
			aEvents := slices.Clone(exp.events[arns[0]])
			exp.mu.Unlock()
			if !slices.Equal(aEvents, []string{"change:start"}) {
				t.Errorf("A emitted before its blocked export finished: %v", aEvents)
			}
			close(exp.gates[arns[0]].release)
			awaitSignal(t, done[0])
			exp.mu.Lock()
			defer exp.mu.Unlock()
			for i, arn := range arns {
				wantEvents := []string{"change:start", "change:finish", "change:start", "change:finish", "end:start", "end:finish"}
				if !slices.Equal(exp.events[arn], wantEvents) {
					t.Errorf("dispatch order for %s = %v, want %v", arn, exp.events[arn], wantEvents)
				}
				records := exp.records[arn]
				if len(records) != 3 {
					t.Fatalf("%s: got %d records, want initial, coalesced and final", arn, len(records))
				}
				if records[0].Status != StatusRunning {
					t.Errorf("initial record status = %s, want RUNNING", records[0].Status)
				}
				for _, rec := range records[1:] {
					if rec.ExecutionArn != arn || len(rec.Operations) != 1 || string(rec.Operations[0].Result) != jsonString(fmt.Sprintf("result-%d", i)) {
						t.Errorf("updated record for %s = %+v", arn, rec)
					}
				}
				final := records[len(records)-1]
				if outcome == durable.PluginInvocationSucceeded {
					if final.Status != StatusSucceeded || final.EndTimestamp == nil {
						t.Errorf("final succeeded record = %+v", final)
					}
				} else if final.Status != StatusRunning || final.EndTimestamp != nil || final.DurationMs != nil || final.Error != nil {
					t.Errorf("final continuing record = %+v", final)
				}
			}
		})
	}
}
