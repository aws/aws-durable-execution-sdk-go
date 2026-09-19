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
	// The child context ends with no parent; the step ends with a parent.
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "parent": "NONE", "status": "SUCCEEDED"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "status": "SUCCEEDED"}, 2)
	for _, r := range recs {
		if r["parent"] == "" {
			t.Errorf("empty parent field: %v", r)
		}
	}
}
