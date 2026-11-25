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
	if len(result.Operations) != 1 {
		t.Fatalf("got %d checkpointed operations, want the one step: %v", len(result.Operations), result.Operations)
	}
	step := result.Operations[0]

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCheckpointedIDs(t, recs, result, "op")
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "op": step.ID, "status": "SUCCEEDED", "in_full_map": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "op": step.ID, "status": "FAILED"}, 0)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "in_full_map": false}, 0)
}

// TestPluginReportsUpdatedSteps drives the hook directly. A step in
// UpdatedOperations emits one record; an operation of another type emits
// none; in_full_map is read from the change's Operations map.
func TestPluginReportsUpdatedSteps(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	p, _ := newOperationChangePlugin(context.Background(), durable.InvocationHookInfo{ExecutionArn: "arn:test"})
	step := durable.OperationHookInfo{ID: "op-1", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationSucceeded}
	absent := durable.OperationHookInfo{ID: "op-3", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationStarted}
	wait := durable.OperationHookInfo{ID: "op-2", Type: string(durable.OperationTypeWait), Status: durable.PluginOperationSucceeded}
	p.OnOperationChange(context.Background(), durable.OperationChangeHookInfo{
		ExecutionArn:      "arn:test",
		UpdatedOperations: map[string]durable.OperationHookInfo{"op-1": step, "op-2": wait, "op-3": absent},
		Operations:        map[string]durable.OperationHookInfo{"op-1": step, "op-2": wait},
	})

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "op": "op-1", "status": "SUCCEEDED", "in_full_map": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "op": "op-3", "status": "STARTED", "in_full_map": false}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "op": "op-2"}, 0)
}
