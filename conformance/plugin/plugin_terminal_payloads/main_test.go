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
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "status": "SUCCEEDED", "error": "NONE"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "status": "FAILED", "result": "NONE", "error": "boom"}, 1)
}
