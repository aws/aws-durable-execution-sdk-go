package telemetry

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func createTestPlugin(t *testing.T, ctx context.Context, factory *TelemetryPlugin, info durable.InvocationHookInfo) *durable.Plugin {
	t.Helper()
	plugin, err := factory.CreatePlugin(ctx, info)
	if err != nil || plugin == nil {
		t.Fatalf("CreatePlugin = (%v, %v), want a plugin without error", plugin, err)
	}
	return plugin
}

func startTestPlugin(t *testing.T, ctx context.Context, factory *TelemetryPlugin, info durable.InvocationHookInfo) *durable.Plugin {
	t.Helper()
	plugin := createTestPlugin(t, ctx, factory, info)
	plugin.OnInvocationStart(ctx, info)
	return plugin
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for test synchronization")
	}
}

func TestPlugin_ConcurrentInvocations_IsolateRecords(t *testing.T) {
	ctx := t.Context()
	exp := &capturingExporter{}
	// EmitOnChange registers OnOperationChange. So the two invocations
	// write their operation lists between the handshake points below.
	factory := New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}, Content: withResults("shared")})
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	infos := []durable.InvocationHookInfo{
		{ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:fn-a:1/durable-execution/name-a/id-a", RequestID: "request-a", ExecutionInput: "input-a", ExecutionStartTimestamp: start},
		{ExecutionArn: "arn:aws:lambda:us-west-2:123456789012:function:fn-b:2/durable-execution/name-b/id-b", RequestID: "request-b", ExecutionInput: "input-b", ExecutionStartTimestamp: start.Add(time.Hour)},
	}
	aStarted, bStarted := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	done := make(chan struct{}, 2)
	logs := make(chan map[string]any, 2)
	for i, info := range infos {
		go func() {
			defer func() { done <- struct{}{} }()
			if i == 1 {
				select {
				case <-aStarted:
				case <-release:
					return
				}
			}
			// This runs off the test goroutine, so t.Fatalf is not allowed
			// here; report the failure with t.Errorf and return instead.
			plugin, err := factory.CreatePlugin(ctx, info)
			if err != nil || plugin == nil {
				t.Errorf("CreatePlugin = (%v, %v), want a plugin without error", plugin, err)
				return
			}
			plugin.OnInvocationStart(ctx, info)
			reportChange(ctx, plugin, durable.OperationHookInfo{ExecutionArn: info.ExecutionArn, ID: "shared-id", Type: "STEP", Status: durable.PluginOperationStarted})
			if i == 0 {
				close(aStarted)
				select {
				case <-bStarted:
				case <-release:
					return
				}
			} else {
				close(bStarted)
			}
			succeeded := durable.OperationHookInfo{ExecutionArn: info.ExecutionArn, ID: "shared-id", Name: "shared", Type: "STEP", Status: durable.PluginOperationSucceeded, Result: fmt.Sprintf("result-%d", i)}
			reportChange(ctx, plugin, succeeded)
			logs <- plugin.EnrichLogContext(ctx)
			end := durable.InvocationEndHookInfo{ExecutionArn: info.ExecutionArn, Status: durable.PluginInvocationSucceeded, ExecutionResult: "output-a", Operations: opsByID(succeeded)}
			if i == 1 {
				end.Status = durable.PluginInvocationFailed
				end.ExecutionResult = nil
				end.ExecutionError = &testError{msg: "error-b"}
			}
			plugin.OnInvocationEnd(ctx, end)
		}()
	}
	awaitSignal(t, done)
	awaitSignal(t, done)
	// Each invocation also emits running records. The record at invocation
	// end is the last one of its execution, so later records replace
	// earlier ones in the map.
	records := exp.snapshot()
	byARN := make(map[string]Record)
	for _, rec := range records {
		byARN[rec.ExecutionArn] = rec
	}
	for i, info := range infos {
		rec, ok := byARN[info.ExecutionArn]
		if !ok {
			t.Fatalf("missing record for %s: %+v", info.ExecutionArn, records)
		}
		if rec.FunctionName != []string{"fn-a", "fn-b"}[i] || rec.ExecutionName != []string{"name-a", "name-b"}[i] {
			t.Errorf("execution %d metadata = %+v", i, rec)
		}
		if string(rec.Input) != jsonString([]string{"input-a", "input-b"}[i]) || rec.StartTimestamp == nil || !rec.StartTimestamp.Equal(info.ExecutionStartTimestamp) {
			t.Errorf("execution %d input/start = %+v / %v", i, rec.Input, rec.StartTimestamp)
		}
		if len(rec.Operations) != 1 || rec.Operations[0].ID != "shared-id" || rec.Operations[0].Status != "SUCCEEDED" || string(rec.Operations[0].Result) != jsonString(fmt.Sprintf("result-%d", i)) {
			t.Errorf("execution %d operations = %+v", i, rec.Operations)
		}
		if i == 0 {
			if rec.Status != StatusSucceeded || string(rec.Output) != jsonString("output-a") || rec.Error != nil {
				t.Errorf("execution A outcome = %+v", rec)
			}
		} else if rec.Status != StatusFailed || rec.Output != nil || rec.Error == nil || rec.Error.Message != "error-b" {
			t.Errorf("execution B outcome = %+v", rec)
		}
	}
	logFields := make(map[string]any)
	for range infos {
		fields := <-logs
		arn, ok := fields["execution_arn"].(string)
		if !ok {
			t.Fatalf("execution ARN missing from log fields: %v", fields)
		}
		logFields[arn] = fields["function_name"]
	}
	for i, info := range infos {
		if logFields[info.ExecutionArn] != []string{"fn-a", "fn-b"}[i] {
			t.Errorf("log metadata for %s = %v", info.ExecutionArn, logFields)
		}
	}
}

// TestPlugin_ConcurrentLogEnrichmentAndChanges runs log enrichment on many
// goroutines while the changes of one invocation are reported. The SDK
// reports the changes one at a time, and handler goroutines can enrich a
// log at any moment. Each change carries one more operation. The final
// record holds every operation of OnInvocationEnd once.
func TestPlugin_ConcurrentLogEnrichmentAndChanges(t *testing.T) {
	ctx := t.Context()
	exp := &capturingExporter{}
	info := durable.InvocationHookInfo{ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/name/id"}
	plugin := startTestPlugin(t, ctx, New(Config{EmitMode: EmitOnChange, Exporters: []Exporter{exp}, Content: withResults("op")}), info)
	const count = 64
	ready := make(chan struct{}, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			if fields := plugin.EnrichLogContext(ctx); fields["execution_arn"] != info.ExecutionArn || fields["function_name"] != "fn" {
				t.Errorf("log fields = %v", fields)
			}
		}()
	}
	for range count {
		awaitSignal(t, ready)
	}
	close(start)
	var ops []durable.OperationHookInfo
	for i := range count {
		id := fmt.Sprintf("op-%d", i)
		ops = append(ops, durable.OperationHookInfo{ID: id, Name: "op", Type: "STEP", Status: durable.PluginOperationSucceeded, Result: id})
		reportChange(ctx, plugin, ops...)
	}
	wg.Wait()
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{Status: durable.PluginInvocationSucceeded, Operations: opsByID(ops...)})
	records := exp.snapshot()
	if len(records) == 0 {
		t.Fatal("no record was exported")
	}
	final := records[len(records)-1]
	if final.Status != StatusSucceeded || len(final.Operations) != count {
		t.Fatalf("final record = %+v, want SUCCEEDED with %d operations", final, count)
	}
	seen := make(map[string]bool)
	for _, op := range final.Operations {
		if seen[op.ID] || op.Status != "SUCCEEDED" || string(op.Result) != jsonString(op.ID) {
			t.Errorf("unexpected operation: %+v", op)
		}
		seen[op.ID] = true
	}
	for i := range count {
		if !seen[fmt.Sprintf("op-%d", i)] {
			t.Errorf("missing operation %d", i)
		}
	}
}
