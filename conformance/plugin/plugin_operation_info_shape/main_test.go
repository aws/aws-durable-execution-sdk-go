package main

import (
	"regexp"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

var iso8601 = regexp.MustCompile(`^\d{4}-`)

// TestHandler mirrors the ExpectedLogs of requirement 10-20: the start
// record carries the identity fields with isReplay false, the end record
// adds status, attempt, both timestamps, and no error key.
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
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2: %v", len(recs), recs)
	}

	start := map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-start", "id": step.ID, "name": "greet", "type": "STEP", "isReplay": false}
	plugintest.ExpectCount(t, recs, start, 1)

	end := map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "id": step.ID, "name": "greet", "type": "STEP", "isReplay": false, "status": "SUCCEEDED", "attempt": 1}
	plugintest.ExpectCount(t, recs, end, 1)
	endRec := recs[pluginlog.First(recs, end)]
	for _, key := range []string{"startTimestamp", "endTimestamp"} {
		if v, _ := endRec[key].(string); !iso8601.MatchString(v) {
			t.Errorf("operation-end %s = %q, want an ISO-8601 timestamp", key, v)
		}
	}
	if _, present := endRec["error"]; present {
		t.Errorf("operation-end carries an error key: %v", endRec)
	}
}
