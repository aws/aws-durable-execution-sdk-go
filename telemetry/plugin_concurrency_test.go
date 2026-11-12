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
	factory := New(Config{Exporters: []Exporter{exp}, Content: allContent()})
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
			plugin.OnOperationStart(ctx, durable.OperationHookInfo{ExecutionArn: info.ExecutionArn, ID: "shared-id", Type: "STEP", Status: durable.PluginOperationStarted})
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
			plugin.OnOperationEnd(ctx, durable.OperationHookInfo{ExecutionArn: info.ExecutionArn, ID: "shared-id", Type: "STEP", Status: durable.PluginOperationSucceeded, Result: fmt.Sprintf("result-%d", i)})
			logs <- plugin.EnrichLogContext(ctx)
			end := durable.InvocationEndHookInfo{ExecutionArn: info.ExecutionArn, Status: durable.PluginInvocationSucceeded, ExecutionResult: "output-a"}
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
	records := exp.snapshot()
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
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

func TestPlugin_ConcurrentOperations_PreserveUpdates(t *testing.T) {
	ctx := t.Context()
	exp := &capturingExporter{}
	info := durable.InvocationHookInfo{ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:fn:1/durable-execution/name/id"}
	plugin := startTestPlugin(t, ctx, New(Config{Exporters: []Exporter{exp}, Content: allContent()}), info)
	const count = 64
	ready := make(chan struct{}, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			id := fmt.Sprintf("op-%d", i)
			plugin.OnOperationStart(ctx, durable.OperationHookInfo{ID: id, Type: "STEP", Status: durable.PluginOperationStarted})
			plugin.OnOperationEnd(ctx, durable.OperationHookInfo{ID: id, Type: "STEP", Status: durable.PluginOperationSucceeded, Result: id})
			if fields := plugin.EnrichLogContext(ctx); fields["execution_arn"] != info.ExecutionArn || fields["function_name"] != "fn" {
				t.Errorf("log fields = %v", fields)
			}
		}()
	}
	for range count {
		awaitSignal(t, ready)
	}
	close(start)
	wg.Wait()
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{Status: durable.PluginInvocationSucceeded})
	records := exp.snapshot()
	if len(records) != 1 || len(records[0].Operations) != count {
		t.Fatalf("records = %+v, want one record with %d operations", records, count)
	}
	seen := make(map[string]bool)
	for _, op := range records[0].Operations {
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
