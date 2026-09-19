package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins((&suspensionPlugin{}).plugin()))
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	suspended := map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-end", "first": true, "terminal": false}
	terminal := map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-end", "first": false, "terminal": true, "status": "SUCCEEDED"}
	plugintest.ExpectCount(t, recs, suspended, 1)
	plugintest.ExpectCount(t, recs, terminal, 1)
	plugintest.ExpectAfter(t, recs, terminal, suspended)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "invocation-end", "first": true, "terminal": true}, 0)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "invocation-end", "status": "FAILED"}, 0)
}
