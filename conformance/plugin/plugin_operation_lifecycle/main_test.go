package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(plugin))
	result := runner.RunUntilComplete(t, "world")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-start"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "status": "SUCCEEDED"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-end", "status": "FAILED"}, 0)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2: %v", len(recs), recs)
	}
	// Both records name the same operation.
	if recs[0]["op"] != recs[1]["op"] || recs[0]["op"] == "" {
		t.Errorf("operation ids differ or are empty: %v", recs)
	}
}
