package main

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

var iso8601 = regexp.MustCompile(`^\d{4}-`)

// TestHandler mirrors the ExpectedLogs of requirement 10-22, except the
// attempt: the step's SUCCEEDED change is reported exactly once and its
// record carries the identity fields, the status, both timestamps, the
// replay indicator, and the fields of the change info. No FAILED change is
// reported.
//
// The attempt is not asserted here. The in-memory client of the local
// runner records attempt 0 for a step that succeeds on its first attempt,
// and InfoRecord omits a zero attempt. TestPluginDumpsAttempt covers the
// attempt key.
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

	succeeded := map[string]any{
		"plugin": "CONFPLUGIN", "hook": "operation-change", "id": step.ID, "name": "greet", "type": "STEP",
		"status": "SUCCEEDED", "inFullMap": true,
	}
	plugintest.ExpectCount(t, recs, succeeded, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "status": "FAILED"}, 0)

	i := pluginlog.First(recs, succeeded)
	if i < 0 {
		t.Fatalf("no SUCCEEDED operation-change record for %s", step.ID)
	}
	rec := recs[i]
	for _, key := range []string{"startTimestamp", "endTimestamp"} {
		if v, _ := rec[key].(string); !iso8601.MatchString(v) {
			t.Errorf("%s = %q, want an ISO-8601 timestamp", key, v)
		}
	}
	if arn, _ := rec["executionArn"].(string); !strings.HasPrefix(arn, "arn:") {
		t.Errorf("executionArn = %q, want an ARN", arn)
	}
	for _, key := range []string{"updatedOperationsCount", "operationsCount"} {
		if n, _ := rec[key].(float64); n < 1 {
			t.Errorf("%s = %v, want at least 1", key, rec[key])
		}
	}
	if _, present := rec["isReplay"]; !present {
		t.Errorf("record carries no isReplay key: %v", rec)
	}
}

// TestPluginDumpsAttempt drives the hook directly with a step entry that
// carries an attempt: the record reports it. An entry of another type
// produces no record.
func TestPluginDumpsAttempt(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	p, _ := newPlugin(context.Background(), durable.InvocationHookInfo{ExecutionArn: "arn:test"})
	step := durable.OperationHookInfo{ID: "op-1", Name: "greet", Type: string(durable.OperationTypeStep), Status: durable.PluginOperationSucceeded, Attempt: 1}
	wait := durable.OperationHookInfo{ID: "op-2", Type: string(durable.OperationTypeWait), Status: durable.PluginOperationSucceeded}
	ops := map[string]durable.OperationHookInfo{"op-1": step, "op-2": wait}
	p.OnOperationChange(context.Background(), durable.OperationChangeHookInfo{ExecutionArn: "arn:test", UpdatedOperations: ops, Operations: ops})

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "id": "op-1", "attempt": 1, "inFullMap": true, "executionArn": "arn:test"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-change", "id": "op-2"}, 0)
}
