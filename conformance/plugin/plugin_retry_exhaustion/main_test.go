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
	s1 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-start", "n": 1}
	e1 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-end", "n": 1, "outcome": "FAILED"}
	s2 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-start", "n": 2}
	e2 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-end", "n": 2, "outcome": "FAILED"}
	end := map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "status": "FAILED"}
	for _, m := range []map[string]any{s1, e1, s2, e2, end} {
		plugintest.ExpectCount(t, recs, m, 1)
	}
	plugintest.ExpectAfter(t, recs, e1, s1)
	plugintest.ExpectAfter(t, recs, s2, e1)
	plugintest.ExpectAfter(t, recs, e2, s2)
	plugintest.ExpectAfter(t, recs, end, s2)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "attempt-start", "n": 3}, 0)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "attempt-end", "outcome": "SUCCEEDED"}, 0)
}
