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
	got, err := durabletest.ResultAs[[]string](result)
	if err != nil || len(got) != 2 || got[0] != "task-1" || got[1] != "task-2" {
		t.Fatalf("result = %v, %v", got, err)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "fn-start"}, 2)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "fn-end", "outcome": "SUCCEEDED"}, 2)
	plugintest.ExpectCount(t, recs, map[string]any{"hook": "fn-end", "outcome": "FAILED"}, 0)
	// Every branch record names the parallel operation as its parent, and
	// each branch's start precedes its end.
	starts := map[string]int{}
	for i, r := range recs {
		if r["parent"] == "NONE" || r["parent"] == "" {
			t.Errorf("branch record without parent: %v", r)
		}
		op, _ := r["op"].(string)
		switch r["hook"] {
		case "fn-start":
			starts[op] = i
		case "fn-end":
			if s, ok := starts[op]; !ok || s > i {
				t.Errorf("fn-end for %s not preceded by fn-start: %v", op, recs)
			}
		}
	}
}
