package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(faulty, healthy))
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
	for _, hook := range []string{"invocation-start", "operation-start", "attempt-start", "attempt-end", "operation-end", "invocation-end"} {
		plugintest.ExpectCount(t, recs, map[string]any{"plugin": faultyLabel, "hook": hook}, 1)
	}
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": healthyLabel, "hook": "invocation-start", "first": true}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": healthyLabel, "hook": "operation-start"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": healthyLabel, "hook": "attempt-start"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": healthyLabel, "hook": "attempt-end", "outcome": "SUCCEEDED"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": healthyLabel, "hook": "operation-end", "status": "SUCCEEDED"}, 1)
	plugintest.ExpectCount(t, recs, map[string]any{"plugin": healthyLabel, "hook": "invocation-end", "status": "SUCCEEDED"}, 1)
	plugintest.ExpectAfter(t, recs,
		map[string]any{"plugin": healthyLabel, "hook": "invocation-end"},
		map[string]any{"plugin": healthyLabel, "hook": "invocation-start"})
}
