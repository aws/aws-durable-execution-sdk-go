package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Both log exporters render operationsByName, so the plugin must size them
// through Render rather than through plain json.Marshal.
var (
	_ RenderingSizeLimiter = (*CloudWatchLogsExporter)(nil)
	_ RenderingSizeLimiter = (*LambdaLogExporter)(nil)
)

// logExporter is an exporter that the plugin sizes through Render.
type logExporter interface {
	Exporter
	RenderingSizeLimiter
}

// logExporterCase builds one log exporter with the given size limit and
// returns a function that reads back every payload it sent.
type logExporterCase struct {
	name  string
	build func(maxSizeBytes int) (logExporter, func() []string)
}

func logExporterCases() []logExporterCase {
	return []logExporterCase{
		{
			name: "CloudWatchLogsExporter",
			build: func(maxSizeBytes int) (logExporter, func() []string) {
				fake := &fakeCloudWatchLogsAPI{}
				exp := &CloudWatchLogsExporter{API: fake, LogGroupName: "/test/telemetry", MaxSizeBytes: maxSizeBytes}
				return exp, func() []string {
					var out []string
					for _, call := range fake.putEventsCalls {
						for _, ev := range call.LogEvents {
							out = append(out, aws.ToString(ev.Message))
						}
					}
					return out
				}
			},
		},
		{
			name: "LambdaLogExporter",
			build: func(maxSizeBytes int) (logExporter, func() []string) {
				var buf bytes.Buffer
				exp := &LambdaLogExporter{Writer: &buf, MaxSizeBytes: maxSizeBytes}
				return exp, func() []string {
					return strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
				}
			},
		},
	}
}

// byNameTestRecord returns a record with a repeated operation name and one
// single-occurrence operation, so the array and by-name forms differ.
func byNameTestRecord() Record {
	start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	d := int64(5)
	rec := *NewRecord("arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/exec-1/inv-1")
	rec.Status = StatusSucceeded
	rec.Operations = []OperationRecord{
		{ID: "1", Name: "poll", Type: "STEP", SubType: "Step", Status: "SUCCEEDED", StartTimestamp: &start, DurationMs: &d, Attempt: 1},
		{ID: "2", Name: "poll", Type: "STEP", SubType: "Step", Status: "SUCCEEDED", StartTimestamp: &start, DurationMs: &d, Attempt: 2},
		{ID: "3", Name: "charge", Type: "STEP", SubType: "Step", Status: "SUCCEEDED", StartTimestamp: &start, DurationMs: &d, Attempt: 1,
			Result: json.RawMessage(`{"ok":true}`)},
	}
	return rec
}

// TestLogExporters_ExportSendsRender checks that each log exporter sends
// exactly its Render output, and that the output is the by-name form: an
// operationsByName map and no operations array.
func TestLogExporters_ExportSendsRender(t *testing.T) {
	for _, tc := range logExporterCases() {
		t.Run(tc.name, func(t *testing.T) {
			exp, sent := tc.build(0)
			rec := byNameTestRecord()
			if err := exp.Export(context.Background(), rec); err != nil {
				t.Fatalf("Export: %v", err)
			}
			want, err := exp.Render(rec)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			got := sent()
			if len(got) != 1 || got[0] != string(want) {
				t.Fatalf("sent %q, want exactly the Render output %q", got, want)
			}
			byNameOnly, err := RenderRecord(rec, OperationsFormatByName)
			if err != nil {
				t.Fatalf("RenderRecord: %v", err)
			}
			if !bytes.Equal(want, byNameOnly) {
				t.Errorf("Render = %s, want RenderRecord(record, OperationsFormatByName) = %s", want, byNameOnly)
			}

			var fields map[string]json.RawMessage
			if err := json.Unmarshal(want, &fields); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if _, ok := fields["operations"]; ok {
				t.Error("rendered record has an operations key; the by-name form replaces it")
			}
			var byName map[string]OperationSummary
			if err := json.Unmarshal(fields["operationsByName"], &byName); err != nil {
				t.Fatalf("operationsByName = %s: %v", fields["operationsByName"], err)
			}
			if byName["poll"].Count != 2 || byName["poll"].MaxAttempt != 2 {
				t.Errorf("operationsByName.poll = %+v, want count 2 and maxAttempt 2", byName["poll"])
			}
			if string(byName["charge"].Result) != `{"ok":true}` {
				t.Errorf("operationsByName.charge.result = %s, want {\"ok\":true}", byName["charge"].Result)
			}
		})
	}
}

// runPluginWithRepeatedSteps drives one invocation that runs n steps that
// share one name, and returns after the terminal record is dispatched.
func runPluginWithRepeatedSteps(t *testing.T, exporters []Exporter, n int, input any) {
	t.Helper()
	ctx := context.Background()
	arn := "arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/exec-1/inv-1"
	now := time.Now()
	plugin := startTestPlugin(t, ctx, New(Config{Exporters: exporters}), durable.InvocationHookInfo{
		ExecutionArn:            arn,
		IsFirstInvocation:       true,
		ExecutionStartTimestamp: now,
		ExecutionInput:          input,
	})
	for i := 0; i < n; i++ {
		info := durable.OperationHookInfo{
			ExecutionArn:   arn,
			ID:             fmt.Sprintf("op-%04d", i),
			Name:           "poll",
			Type:           "STEP",
			SubType:        "Step",
			Status:         durable.PluginOperationStarted,
			Attempt:        1,
			StartTimestamp: now.Add(time.Duration(i) * time.Millisecond),
		}
		plugin.OnOperationStart(ctx, info)
		info.Status = durable.PluginOperationSucceeded
		info.EndTimestamp = info.StartTimestamp.Add(2 * time.Millisecond)
		plugin.OnOperationEnd(ctx, info)
	}
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn: arn,
		Status:       durable.PluginInvocationSucceeded,
	})
}

// TestLogExporters_SizedAgainstByNameRendering checks that the plugin
// sizes each log exporter against the by-name bytes it sends. The record
// has 200 operations with one name. Its array form is over the limit, and
// its by-name form, one summary, is under it. So the exporter receives
// the record whole. A sizing pass over the array form would have dropped
// operations and set truncated.
//
// A capturing exporter in the same dispatch has no size limit. It must
// receive the untruncated record, because each limited exporter gets its
// own truncated copy.
func TestLogExporters_SizedAgainstByNameRendering(t *testing.T) {
	const limit = 4096
	for _, tc := range logExporterCases() {
		t.Run(tc.name, func(t *testing.T) {
			exp, sent := tc.build(limit)
			capture := &capturingExporter{}
			runPluginWithRepeatedSteps(t, []Exporter{exp, capture}, 200, map[string]any{"orderId": "abc"})

			records := capture.snapshot()
			if len(records) != 1 {
				t.Fatalf("capturing exporter got %d records, want 1", len(records))
			}
			arrayForm, err := RenderRecord(records[0], OperationsFormatArray)
			if err != nil {
				t.Fatalf("RenderRecord: %v", err)
			}
			if len(arrayForm) <= limit {
				t.Fatalf("array form is %d bytes; the test needs it over the %d-byte limit", len(arrayForm), limit)
			}
			if len(records[0].Operations) != 200 || records[0].Truncated {
				t.Errorf("capturing exporter got %d operations, truncated=%v; want all 200, untruncated",
					len(records[0].Operations), records[0].Truncated)
			}

			got := sent()
			if len(got) != 1 {
				t.Fatalf("log exporter sent %d payloads, want 1", len(got))
			}
			if len(got[0]) > limit {
				t.Errorf("sent payload is %d bytes, over the %d-byte limit", len(got[0]), limit)
			}
			fields := decodeWithNumbers(t, got[0])
			if _, ok := fields["truncated"]; ok {
				t.Errorf("sent payload has truncated=%v; its by-name form fits, so nothing should be dropped", fields["truncated"])
			}
			byName, _ := fields["operationsByName"].(map[string]any)
			poll, _ := byName["poll"].(map[string]any)
			assertInteger(t, "operationsByName.poll.count", poll["count"], 200)
		})
	}
}

// TestLogExporters_TruncatesOverLimitByNameRendering checks the other
// side: when the by-name form itself is over the limit, the sent payload
// fits the limit and is marked truncated. The input is three times the
// limit, so dropping operations cannot make the record fit. So Truncate
// drops all three operations, then the input. The payload must still be
// the by-name form. With no operations left, that form is an empty
// operationsByName object, while the array form would be an empty
// operations array. So the test asserts the empty object and the absent
// array.
func TestLogExporters_TruncatesOverLimitByNameRendering(t *testing.T) {
	const limit = 2048
	for _, tc := range logExporterCases() {
		t.Run(tc.name, func(t *testing.T) {
			exp, sent := tc.build(limit)
			capture := &capturingExporter{}
			runPluginWithRepeatedSteps(t, []Exporter{exp, capture}, 3, map[string]any{"blob": strings.Repeat("x", 3*limit)})

			got := sent()
			if len(got) != 1 {
				t.Fatalf("log exporter sent %d payloads, want 1", len(got))
			}
			if len(got[0]) > limit {
				t.Errorf("sent payload is %d bytes, over the %d-byte limit", len(got[0]), limit)
			}
			fields := decodeWithNumbers(t, got[0])
			if fields["truncated"] != true || fields["droppedInput"] != true {
				t.Errorf("truncated=%v droppedInput=%v, want both true", fields["truncated"], fields["droppedInput"])
			}
			if _, ok := fields["input"]; ok {
				t.Error("sent payload still has input; Truncate should have dropped it")
			}
			assertInteger(t, "droppedOperations", fields["droppedOperations"], 3)
			if _, ok := fields["operations"]; ok {
				t.Error("sent payload has an operations key; the by-name form replaces it")
			}
			if byName, ok := fields["operationsByName"].(map[string]any); !ok || len(byName) != 0 {
				t.Errorf("operationsByName = %#v, want an empty object after every operation is dropped", fields["operationsByName"])
			}

			records := capture.snapshot()
			if len(records) != 1 || len(records[0].Input) <= 3*limit || records[0].Truncated {
				t.Errorf("capturing exporter must receive the untruncated input")
			}
		})
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
