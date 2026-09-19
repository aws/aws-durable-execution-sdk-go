package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins((&replayFlagsPlugin{}).plugin()))
	result := runner.RunUntilComplete(t, nil)
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	// Two live starts (one per step), at least one replayed start (step B
	// on the retry invocation), and exactly one terminal end per step.
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-start", "replay": false}, 2)
	plugintest.ExpectMinCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-start", "replay": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "operation-end", "status": "SUCCEEDED"}, 2)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-end", "status": "FAILED"}, 0)
	// The replayed start belongs to step B, the step whose live start came
	// second; step A is terminal before the replay and is never re-emitted.
	var stepA, stepB string
	for _, r := range recs {
		if r["hook"] == "operation-start" && r["replay"] == false {
			if stepA == "" {
				stepA, _ = r["op"].(string)
			} else {
				stepB, _ = r["op"].(string)
			}
		}
	}
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-start", "op": stepA, "replay": true}, 0)
	plugintest.ExpectMinCount(t, recs, map[string]any{"hook": "operation-start", "op": stepB, "replay": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-end", "op": stepA}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "operation-end", "op": stepB}, 1)
}
