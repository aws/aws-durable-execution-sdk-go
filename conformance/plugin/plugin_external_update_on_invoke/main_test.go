package main

import (
	"context"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandler runs the handler end to end. The local runner does not
// report externally updated operations to the invocation-start hook, so
// only the absence of records on the first invocation is asserted here.
func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(plugin))
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	plugintest.ExpectCount(t, records(), map[string]any{"hook": "updated-on-invoke", "first": true}, 0)
}

// TestPluginReportsUpdatedWaits drives the hook directly with the shape the
// SDK delivers when a wait completed between invocations.
func TestPluginReportsUpdatedWaits(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	wait := durable.OperationHookInfo{ID: "op-1", Type: string(durable.OperationTypeWait), Status: durable.PluginOperationSucceeded}
	step := durable.OperationHookInfo{ID: "op-2", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationSucceeded}
	plugin.OnInvocationStart(context.Background(), durable.InvocationHookInfo{
		ExecutionArn:      "arn:test",
		IsFirstInvocation: false,
		UpdatedOperations: map[string]durable.OperationHookInfo{"op-1": wait, "op-2": step},
	})

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "updated-on-invoke", "op": "op-1", "status": "SUCCEEDED", "first": false}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "updated-on-invoke", "op": "op-2"}, 0)
}
