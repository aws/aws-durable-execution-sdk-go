package main

import (
	"context"
	"regexp"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

var iso8601 = regexp.MustCompile(`^\d{4}-`)

// TestHandler mirrors the ExpectedLogs of requirement 10-22: exactly one
// record reports the step as SUCCEEDED, with the item's identity fields,
// both timestamps, an isReplay key, and the hook-level fields. No record
// reports the step as FAILED.
//
// The requirement also asserts attempt 1. The item's attempt is the count
// the checkpoint response carries. The local runner's in-memory client
// reports 0 for a step that succeeds on its first attempt, and the dump
// omits a zero attempt. So this test does not assert the attempt key;
// TestPluginDumpsAttempt covers it.
func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(durable.PluginFactoryFunc(newPlugin)))
	result, err := runner.RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	step := result.Operation("greet")
	if step == nil {
		t.Fatalf("no checkpointed operation named greet: %v", result.Operations)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCheckpointedIDs(t, recs, result, "id", "parentId")

	succeeded := map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-change", "id": step.ID, "name": "greet", "type": "STEP", "status": "SUCCEEDED", "inFullMap": true}
	plugintest.ExpectCount(t, recs, succeeded, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "id": step.ID, "status": "FAILED"}, 0)
	i := pluginlog.First(recs, succeeded)
	if i < 0 {
		t.Fatalf("no SUCCEEDED operation-change record: %v", recs)
	}
	rec := recs[i]
	for _, key := range []string{"startTimestamp", "endTimestamp"} {
		if v, _ := rec[key].(string); !iso8601.MatchString(v) {
			t.Errorf("%s = %q, want an ISO-8601 timestamp", key, v)
		}
	}
	if v, _ := rec["executionArn"].(string); v == "" {
		t.Errorf("executionArn is absent or empty: %v", rec)
	}
	for _, key := range []string{"updatedOperationsCount", "operationsCount"} {
		if v, _ := rec[key].(float64); v < 1 {
			t.Errorf("%s = %v, want at least 1", key, rec[key])
		}
	}
	if _, present := rec["isReplay"]; !present {
		t.Errorf("record carries no isReplay key: %v", rec)
	}
}

// TestPluginDumpsAttempt drives the hook directly with an item that
// carries an attempt count, and asserts the record reports it. A step in
// UpdatedOperations that Operations lacks is reported with inFullMap
// false, and an operation of another type emits no record.
func TestPluginDumpsAttempt(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	p, _ := newPlugin(context.Background(), durable.InvocationHookInfo{ExecutionArn: "arn:test"})
	step := durable.OperationHookInfo{ID: "op-1", Name: "greet", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationSucceeded, Attempt: 1}
	absent := durable.OperationHookInfo{ID: "op-3", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationStarted}
	wait := durable.OperationHookInfo{ID: "op-2", Type: string(durable.OperationTypeWait), Status: durable.PluginOperationSucceeded}
	p.OnOperationChange(context.Background(), durable.OperationChangeHookInfo{
		ExecutionArn:      "arn:test",
		UpdatedOperations: map[string]durable.OperationHookInfo{"op-1": step, "op-2": wait, "op-3": absent},
		Operations:        map[string]durable.OperationHookInfo{"op-1": step, "op-2": wait},
	})

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "id": "op-1", "status": "SUCCEEDED", "attempt": 1, "inFullMap": true, "executionArn": "arn:test", "updatedOperationsCount": 3, "operationsCount": 2}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "id": "op-3", "inFullMap": false}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "id": "op-2"}, 0)
}
