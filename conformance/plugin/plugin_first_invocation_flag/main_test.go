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
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	first := map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-start", "first": true}
	replay := map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-start", "first": false}
	end := map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-end", "status": "SUCCEEDED"}
	plugintest.ExpectCount(t, recs, first, 1)
	plugintest.ExpectCount(t, recs, replay, 1)
	plugintest.ExpectCount(t, recs, end, 1)
	plugintest.ExpectAfter(t, recs, replay, first)
	plugintest.ExpectAfter(t, recs, end, replay)
}
