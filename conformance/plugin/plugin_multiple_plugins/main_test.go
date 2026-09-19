package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(lifecyclePlugin("CONFPLUGIN-A"), lifecyclePlugin("CONFPLUGIN-B")))
	result := runner.RunUntilComplete(t, "world")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	for _, label := range []string{"CONFPLUGIN-A", "CONFPLUGIN-B"} {
		start := map[string]any{"plugin": label, "hook": "invocation-start"}
		end := map[string]any{"plugin": label, "hook": "invocation-end", "status": "SUCCEEDED"}
		plugintest.ExpectCount(t, recs, start, 1)
		plugintest.ExpectCount(t, recs, end, 1)
		plugintest.ExpectAfter(t, recs, end, start)
	}
}
