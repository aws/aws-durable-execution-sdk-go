package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

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

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCheckpointedIDs(t, recs, result, "op", "parent")
	s1 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-start", "n": 1}
	e1 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-end", "n": 1, "outcome": "FAILED"}
	s2 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-start", "n": 2}
	e2 := map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-end", "n": 2, "outcome": "SUCCEEDED"}
	for _, m := range []map[string]any{s1, e1, s2, e2} {
		plugintest.ExpectCount(t, recs, m, 1)
	}
	plugintest.ExpectAfter(t, recs, e1, s1)
	plugintest.ExpectAfter(t, recs, s2, e1)
	plugintest.ExpectAfter(t, recs, e2, s2)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "attempt-start", "n": 3}, 0)
}
