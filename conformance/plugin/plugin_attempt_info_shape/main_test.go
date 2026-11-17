package main

import (
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/pluginlog"
	"github.com/aws/aws-durable-execution-sdk-go/conformance/internal/plugintest"
	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestHandler mirrors the ExpectedLogs of requirement 10-21. Attempt 1
// fails and attempt 2 succeeds. Each attempt-start record carries the
// identity fields and a start timestamp. Each attempt-end record adds the
// outcome and an end timestamp, and only the failed one an error.
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
	if got, err := durabletest.ResultAs[string](result); err != nil || got != "ok" {
		t.Fatalf("result = %q, %v; want ok", got, err)
	}

	step := result.Operation("flaky")
	if step == nil {
		t.Fatalf("no checkpointed operation named flaky: %v", result.Operations)
	}

	recs := records()
	plugintest.ExpectAllStamped(t, recs)
	plugintest.ExpectCheckpointedIDs(t, recs, result, "id", "parentId")

	identity := func(hook string, attempt int) map[string]any {
		return map[string]any{"plugin": "CONFPLUGIN", "hook": hook, "id": step.ID, "name": "flaky", "type": "STEP", "attempt": attempt}
	}
	start1, end1 := identity("attempt-start", 1), identity("attempt-end", 1)
	start2, end2 := identity("attempt-start", 2), identity("attempt-end", 2)
	end1["outcome"] = "FAILED"
	end2["outcome"] = "SUCCEEDED"

	for _, m := range []map[string]any{start1, end1, start2, end2} {
		plugintest.ExpectCount(t, recs, m, 1)
	}
	plugintest.ExpectAfter(t, recs, end1, start1)
	plugintest.ExpectAfter(t, recs, start2, end1)
	plugintest.ExpectAfter(t, recs, end2, start2)

	// timestamp returns the time that the first record matching m carries
	// under key. A missing record and an unparsable value each fail the
	// test here, so no later comparison runs on a zero time unnoticed.
	timestamp := func(m map[string]any, key string) (time.Time, bool) {
		t.Helper()
		i := pluginlog.First(recs, m)
		if i < 0 {
			t.Errorf("no %s record for attempt %v", m["hook"], m["attempt"])
			return time.Time{}, false
		}
		v, _ := recs[i][key].(string)
		ts, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t.Errorf("%s attempt %v: %s = %q, want an RFC 3339 timestamp", m["hook"], m["attempt"], key, v)
			return time.Time{}, false
		}
		return ts, true
	}
	for _, attempt := range []struct {
		n          int
		start, end map[string]any
	}{{1, start1, end1}, {2, start2, end2}} {
		started, okStarted := timestamp(attempt.start, "startTimestamp")
		endStarted, okEndStarted := timestamp(attempt.end, "startTimestamp")
		ended, okEnded := timestamp(attempt.end, "endTimestamp")
		// The attempt-end info reports the start of its own attempt, so it
		// must equal the value the attempt-start hook received.
		if okStarted && okEndStarted && !endStarted.Equal(started) {
			t.Errorf("attempt %d: attempt-end startTimestamp = %s, want the attempt-start value %s", attempt.n, endStarted, started)
		}
		if okEndStarted && okEnded && ended.Before(endStarted) {
			t.Errorf("attempt %d: endTimestamp %s is before startTimestamp %s", attempt.n, ended, endStarted)
		}
	}

	if i := pluginlog.First(recs, end1); i >= 0 {
		if v, _ := recs[i]["error"].(string); v == "" {
			t.Errorf("attempt-end 1 has no error message: %v", recs[i])
		}
	}

	plugintest.ExpectCount(t, recs, map[string]any{"plugin": "CONFPLUGIN", "hook": "attempt-start", "attempt": 3}, 0)
	for _, r := range recs {
		if _, hasErr := r["error"]; hasErr && r["hook"] == "attempt-end" && r["outcome"] == "SUCCEEDED" {
			t.Errorf("succeeded attempt-end carries an error: %v", r)
		}
	}
}
