package main

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandler mirrors the ExpectedLogs of requirement 10-8: the step's
// SUCCEEDED change is reported exactly once, with the checkpointed
// operation ID and in_full_map true, and no FAILED change is reported.
func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(durable.PluginFactoryFunc(newOperationChangePlugin)))
	result, err := runner.RunUntilComplete("world")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCheckpointedIDs(t, recs, result, "op")
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "status": "SUCCEEDED", "in_full_map": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "status": "FAILED"}, 0)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "in_full_map": false}, 0)
}

// TestPluginReportsUpdatedSteps drives the hook directly: a step in
// UpdatedOperations is reported, an operation of another type is not, and
// in_full_map states whether Operations holds the step.
func TestPluginReportsUpdatedSteps(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	p, _ := newOperationChangePlugin(context.Background(), durable.InvocationHookInfo{ExecutionArn: "arn:test"})
	step := durable.OperationHookInfo{ID: "op-1", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationSucceeded}
	wait := durable.OperationHookInfo{ID: "op-2", Type: string(durable.OperationTypeWait), Status: durable.PluginOperationSucceeded}
	ops := map[string]durable.OperationHookInfo{"op-1": step, "op-2": wait}
	p.OnOperationChange(context.Background(), durable.OperationChangeHookInfo{ExecutionArn: "arn:test", UpdatedOperations: ops, Operations: ops})

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "op": "op-1", "status": "SUCCEEDED", "in_full_map": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "op": "op-2"}, 0)
}
