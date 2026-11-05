package main

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func TestHandler(t *testing.T) {
	records := plugintest.RecordTo(t, out)
	runner := durabletest.NewLocalRunner(handler, durable.WithPlugins(durable.PluginFactoryFunc(newFaulty)))
	result, err := runner.RunUntilComplete("world")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED", result.Status)
	}
	got, err := durabletest.ResultAs[string](result)
	if err != nil || got != "Hello, world!" {
		t.Fatalf("result = %q, %v", got, err)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	for _, hook := range []string{"invocation-start", "invocation-end", "operation-start", "operation-end", "attempt-start", "attempt-end"} {
		plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": hook}, 1)
	}
	plugintest.ExpectAfter(t, recs,
		map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "invocation-end"},
		map[string]any{"plugin": "CONFPLUGIN-FAULTY", "hook": "invocation-start"})
}
