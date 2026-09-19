package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandler runs the handler end to end. The local runner completes
// every pending timer at once, so both waits complete before the first
// replay and the pending-replay observation of the long wait does not
// occur locally; the live starts, the terminal ends, and the absence of
// replayed starts for terminal waits are asserted.
func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(plugin))
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	got, err := durabletest.ResultAs[[]string](result)
	if err != nil || len(got) != 2 || got[0] != "short-done" || got[1] != "long-done" {
		t.Fatalf("result = %v, %v", got, err)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	for _, name := range []string{"short", "long"} {
		plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-start", "type": "WAIT", "name": name, "replay": false, "pending": true}, 1)
		plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "type": "WAIT", "name": name, "status": "SUCCEEDED"}, 1)
	}
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-start", "replay": true, "pending": false}, 0)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-end", "status": "FAILED"}, 0)
}
