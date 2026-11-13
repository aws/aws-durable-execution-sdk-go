package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandler mirrors the ExpectedLogs of requirement 10-23. The local
// runner completes the pending wait and replays, so branch-a's start fires
// twice: live with isReplayingChildren false, then re-entered with true.
// branch-b runs live once during the replay invocation.
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
	got, err := durabletest.ResultAs[[]string](result)
	if err != nil || len(got) != 2 || got[0] != "a-done" || got[1] != "b-done" {
		t.Fatalf("result = %v, %v", got, err)
	}

	parent := result.Operation("ctx")
	if parent == nil {
		t.Fatalf("no checkpointed operation named ctx: %v", result.Operations)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCheckpointedIDs(t, recs, result, "id", "parentId")

	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-start", "id": parent.ID, "type": "CONTEXT", "subType": "Parallel", "isReplay": false}, 1)
	for _, name := range []string{"branch-a", "branch-b"} {
		plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-start", "name": name, "type": "CONTEXT", "subType": "ParallelBranch", "parentId": parent.ID, "isReplay": false}, 1)
		plugintest.ExpectMinCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "fn-start", "name": name, "isReplayingChildren": false}, 1)
	}
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "fn-start", "name": "branch-a", "isReplayingChildren": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "fn-start", "name": "branch-b"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "fn-start", "subType": "Parallel"}, 0)
}
