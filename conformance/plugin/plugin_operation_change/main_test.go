package main

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandler runs the handler end to end. The single step reaches its
// terminal status inside the first invocation, which the Go SDK does not
// report through OnOperationChange, so no record is expected.
func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins((&operationChangePlugin{}).plugin()))
	result := runner.RunUntilComplete(t, "world")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	if recs := records(); len(recs) != 0 {
		t.Errorf("expected no records, got %v", recs)
	}
}

// TestPluginReportsUpdatedSteps drives the hooks directly with the shape
// the SDK delivers when a step changed between invocations.
func TestPluginReportsUpdatedSteps(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	p := (&operationChangePlugin{}).plugin()
	step := durable.OperationHookInfo{ID: "op-1", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationSucceeded}
	wait := durable.OperationHookInfo{ID: "op-2", Type: string(durable.OperationTypeWait), Status: durable.PluginOperationSucceeded}
	ops := map[string]durable.OperationHookInfo{"op-1": step, "op-2": wait}
	p.OnInvocationStart(context.Background(), durable.InvocationHookInfo{ExecutionArn: "arn:test", Operations: ops, UpdatedOperations: ops})
	p.OnOperationChange(context.Background(), durable.OperationChangeHookInfo{ExecutionArn: "arn:test", UpdatedOperations: ops})

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "op": "op-1", "status": "SUCCEEDED", "in_full_map": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "op": "op-2"}, 0)
}

// TestPluginCountsChangedStepAsInFullMap covers a step that reaches its
// terminal status inside the invocation: the start snapshot holds only the
// execution operation, and the change reports the step. The step counts as
// present in the full map because the change itself carries it.
func TestPluginCountsChangedStepAsInFullMap(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	p := (&operationChangePlugin{}).plugin()
	exec := durable.OperationHookInfo{ID: "exec", Type: string(durable.OperationTypeExecution), Status: durable.PluginOperationStarted}
	step := durable.OperationHookInfo{ID: "op-1", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationSucceeded}
	p.OnInvocationStart(context.Background(), durable.InvocationHookInfo{
		ExecutionArn: "arn:test",
		Operations:   map[string]durable.OperationHookInfo{"exec": exec},
	})
	p.OnOperationChange(context.Background(), durable.OperationChangeHookInfo{
		ExecutionArn:      "arn:test",
		UpdatedOperations: map[string]durable.OperationHookInfo{"op-1": step},
	})

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "op": "op-1", "status": "SUCCEEDED", "in_full_map": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "in_full_map": false}, 0)
}
