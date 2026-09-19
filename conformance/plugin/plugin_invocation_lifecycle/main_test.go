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
	got, err := durabletest.ResultAs[string](result)
	if err != nil || got != "Hello, world!" {
		t.Fatalf("result = %q, %v", got, err)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	start := map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-start", "first": true}
	end := map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-end", "status": "SUCCEEDED"}
	plugintest.ExpectCount(t, recs, start, 1)
	plugintest.ExpectCount(t, recs, end, 1)
	plugintest.ExpectAfter(t, recs, end, start)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "invocation-start", "first": false}, 0)
}
