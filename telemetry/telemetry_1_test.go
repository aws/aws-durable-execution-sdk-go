package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// telemetry1Input and telemetry1Output are the event and result types of
// the telemetry-1 conformance handler (conformance-telemetry/telemetry_1_success).
type telemetry1Input struct {
	Value int `json:"value"`
}

type telemetry1Output struct {
	Value int `json:"value"`
}

// TestTelemetry1_CloudWatchRecordShape is the local regression test for the
// telemetry-1 conformance requirement
// (aws-durable-execution-conformance-tests,
// test-requirements/telemetry/telemetry-1.yaml). The conformance validator
// requires exactly the listed keys at every nesting level, and it requires
// JSON integers where the requirement says {$integer}. So this test checks
// the complete key set at each level and the JSON type of every integer.
//
// The test drives the plugin hooks directly with the values the durable
// runtime reports for the handler's single "increment" step. The plugin
// configuration is the handler's: on-complete, top-level, sampling rate 1,
// one CloudWatchLogsExporter, and the zero-value ContentConfig.
//
// The step runs for 40 ms. A 0 ms step currently omits minDurationMs,
// maxDurationMs, and totalDurationMs from operationsByName, because the
// OperationSummary duration fields are int64 with omitempty. That case is
// fixed and tested separately (#155).
func TestTelemetry1_CloudWatchRecordShape(t *testing.T) {
	fake := &fakeCloudWatchLogsAPI{}
	exporter := &CloudWatchLogsExporter{API: fake, LogGroupName: "/conformance/telemetry"}
	factory := New(Config{
		EmitMode:        EmitOnComplete,
		OperationDetail: OperationDetailTopLevel,
		SamplingRate:    1,
		Exporters:       []Exporter{exporter},
	})

	ctx := context.Background()
	arn := "arn:aws:lambda:us-west-2:123456789012:function:telemetry-1:$LATEST/durable-execution/exec-1/inv-1"
	start := time.Now().Add(-time.Second)
	stepStart := start.Add(5 * time.Millisecond)
	stepEnd := stepStart.Add(40 * time.Millisecond)

	plugin := startTestPlugin(t, ctx, factory, durable.InvocationHookInfo{
		ExecutionArn:            arn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: start,
		ExecutionInput:          telemetry1Input{Value: 41},
	})
	step := durable.OperationHookInfo{
		ExecutionArn:   arn,
		ID:             "c4ca4238a0b92382",
		Name:           "increment",
		Type:           "STEP",
		SubType:        "Step",
		Status:         durable.PluginOperationStarted,
		Attempt:        1,
		StartTimestamp: stepStart,
	}
	plugin.OnOperationStart(ctx, step)
	step.Status = durable.PluginOperationSucceeded
	step.EndTimestamp = stepEnd
	step.Result = "42"
	plugin.OnOperationEnd(ctx, step)
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn:    arn,
		Status:          durable.PluginInvocationSucceeded,
		ExecutionResult: telemetry1Output{Value: 42},
	})

	if len(fake.putEventsCalls) != 1 || len(fake.putEventsCalls[0].LogEvents) != 1 {
		t.Fatalf("PutLogEvents calls = %d, want exactly 1 call with 1 event", len(fake.putEventsCalls))
	}
	record := decodeWithNumbers(t, aws.ToString(fake.putEventsCalls[0].LogEvents[0].Message))

	assertKeys(t, "record", record,
		"recordType", "schemaVersion", "emittedAt", "executionArn", "executionName",
		"functionName", "functionQualifier", "region", "accountId", "status",
		"startTime", "endTime", "durationMs", "input", "output", "operationsByName")

	wantStrings := map[string]string{
		"recordType":        "DurableExecutionTelemetry",
		"schemaVersion":     "1.0",
		"executionArn":      arn,
		"executionName":     "exec-1",
		"functionName":      "telemetry-1",
		"functionQualifier": "$LATEST",
		"region":            "us-west-2",
		"accountId":         "123456789012",
		"status":            "SUCCEEDED",
	}
	for key, want := range wantStrings {
		if got := record[key]; got != want {
			t.Errorf("%s = %#v, want %q", key, got, want)
		}
	}
	for _, key := range []string{"emittedAt", "startTime", "endTime"} {
		s, ok := record[key].(string)
		if !ok || !timestampPattern.MatchString(s) {
			t.Errorf("%s = %#v, want a UTC timestamp with exactly three fraction digits and a Z suffix", key, record[key])
		}
	}
	assertNonNegativeInteger(t, "durationMs", record["durationMs"])

	for _, key := range []string{"input", "output"} {
		value, ok := record[key].(map[string]any)
		if !ok {
			t.Errorf("%s = %#v, want a JSON object", key, record[key])
			continue
		}
		assertKeys(t, key, value, "value")
	}
	if in, ok := record["input"].(map[string]any); ok {
		assertInteger(t, "input.value", in["value"], 41)
	}
	if out, ok := record["output"].(map[string]any); ok {
		assertInteger(t, "output.value", out["value"], 42)
	}

	byName, ok := record["operationsByName"].(map[string]any)
	if !ok {
		t.Fatalf("operationsByName = %#v, want a JSON object", record["operationsByName"])
	}
	assertKeys(t, "operationsByName", byName, "increment")
	increment, ok := byName["increment"].(map[string]any)
	if !ok {
		t.Fatalf("operationsByName.increment = %#v, want a JSON object", byName["increment"])
	}
	// The zero-value ContentConfig records no operation result, so the
	// summary has no result key.
	assertKeys(t, "operationsByName.increment", increment,
		"type", "subType", "count", "failedCount", "status",
		"minDurationMs", "maxDurationMs", "totalDurationMs", "maxAttempt")
	if increment["type"] != "STEP" || increment["subType"] != "Step" || increment["status"] != "SUCCEEDED" {
		t.Errorf("increment type/subType/status = %#v/%#v/%#v, want STEP/Step/SUCCEEDED",
			increment["type"], increment["subType"], increment["status"])
	}
	assertInteger(t, "increment.count", increment["count"], 1)
	assertInteger(t, "increment.failedCount", increment["failedCount"], 0)
	assertInteger(t, "increment.maxAttempt", increment["maxAttempt"], 1)
	for _, key := range []string{"minDurationMs", "maxDurationMs", "totalDurationMs"} {
		assertInteger(t, "increment."+key, increment[key], 40)
	}
}

// decodeWithNumbers decodes a JSON object with Decoder.UseNumber, so every
// number keeps its literal text and an integer can be told from 41.0.
func decodeWithNumbers(t *testing.T, s string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return out
}

// assertKeys checks that m has exactly the keys want, in any order.
func assertKeys(t *testing.T, path string, m map[string]any, want ...string) {
	t.Helper()
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Errorf("%s keys = %v, want %v", path, got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s keys = %v, want %v", path, got, want)
			return
		}
	}
}

// parseInteger returns v as an int64 when v is a JSON integer literal: a
// json.Number with no fraction and no exponent.
func parseInteger(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(n.String(), 10, 64)
	return i, err == nil
}

func assertInteger(t *testing.T, path string, v any, want int64) {
	t.Helper()
	got, ok := parseInteger(v)
	if !ok || got != want {
		t.Errorf("%s = %#v, want the JSON integer %d", path, v, want)
	}
}

func assertNonNegativeInteger(t *testing.T, path string, v any) {
	t.Helper()
	got, ok := parseInteger(v)
	if !ok || got < 0 {
		t.Errorf("%s = %#v, want a JSON integer >= 0", path, v)
	}
}
