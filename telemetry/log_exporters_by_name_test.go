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
	// suffix is what the exporter's Render appends to the by-name JSON.
	suffix string
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
					// Each payload keeps its newline, because the size
					// limit counts it. SplitAfter leaves one empty string
					// after the last newline, which is not a payload.
					lines := strings.SplitAfter(buf.String(), "\n")
					return lines[:len(lines)-1]
				}
			},
			suffix: "\n",
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
			if wantRender := string(byNameOnly) + tc.suffix; string(want) != wantRender {
				t.Errorf("Render = %q, want RenderRecord(record, OperationsFormatByName) plus %q = %q", want, tc.suffix, wantRender)
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
	ops := make(map[string]durable.OperationHookInfo, n)
	for i := 0; i < n; i++ {
		start := now.Add(time.Duration(i) * time.Millisecond)
		id := fmt.Sprintf("op-%04d", i)
		ops[id] = durable.OperationHookInfo{
			ExecutionArn:   arn,
			ID:             id,
			Name:           "poll",
			Type:           "STEP",
			SubType:        "Step",
			Status:         durable.PluginOperationSucceeded,
			Attempt:        1,
			StartTimestamp: start,
			EndTimestamp:   start.Add(2 * time.Millisecond),
		}
	}
	plugin.OnInvocationEnd(ctx, durable.InvocationEndHookInfo{
		ExecutionArn: arn,
		Status:       durable.PluginInvocationSucceeded,
		Operations:   ops,
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

// TestLogExporters_TruncateByNameCanDropMoreThanNeeded checks the accepted
// trade-off of the binary search in Truncate, when the size is measured on
// the by-name rendering.
//
// The by-name rendering shows an operation's Error only when its name
// occurs once. The record has three operations, oldest first: "big" with a
// large Error, then two operations named "repeated" with a smaller Error
// each. The rendered size for each number of dropped operations is:
//
//   - 0 dropped: over the limit, because the Error of "big" is rendered.
//   - 1 dropped: exactly the limit. "repeated" occurs twice, so neither
//     Error is rendered.
//   - 2 dropped: over the limit. "repeated" now occurs once, so its Error
//     is rendered.
//   - 3 dropped: under the limit.
//
// So the size does not shrink with every dropped operation. The fewest
// drops that fit is 1. A binary search checks 2 first, which does not fit,
// so it moves up and returns 3. Truncate accepts this. So the test checks
// only that Truncate drops at least the fewest and that the result fits.
func TestLogExporters_TruncateByNameCanDropMoreThanNeeded(t *testing.T) {
	for _, tc := range logExporterCases() {
		t.Run(tc.name, func(t *testing.T) {
			exp, _ := tc.build(0)

			failed := func(id, name string, messageLen int) OperationRecord {
				return OperationRecord{
					ID: id, Name: name, Type: "STEP", SubType: "Step", Status: "FAILED", Attempt: 1,
					Error: &ErrorRecord{Type: "TestError", Message: strings.Repeat("e", messageLen)},
				}
			}
			rec := *NewRecord("arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/exec-1/inv-1")
			rec.Status = StatusFailed
			rec.Operations = []OperationRecord{
				failed("1", "big", 2000),
				failed("2", "repeated", 300),
				failed("3", "repeated", 300),
			}

			// The limit is the rendered size of the record with only the
			// oldest operation dropped.
			oneDropped := rec
			oneDropped.Operations = rec.Operations[1:]
			oneDropped.Truncated = true
			oneDropped.DroppedOperations = 1
			want, err := exp.Render(oneDropped)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			limit := len(want)

			fewest := linearDropCount(rec, limit, exp.Render)
			if fewest != 1 {
				t.Fatalf("fewest drops that fit = %d, want 1", fewest)
			}

			out := Truncate(rec, limit, exp.Render)

			if out.DroppedOperations < fewest || out.DroppedOperations+len(out.Operations) != 3 {
				t.Fatalf("DroppedOperations = %d with %d operations kept, want at least %d dropped of 3",
					out.DroppedOperations, len(out.Operations), fewest)
			}
			got, err := exp.Render(out)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if len(got) > limit {
				t.Errorf("rendered record is %d bytes, over the %d-byte limit", len(got), limit)
			}
		})
	}
}

// TestLambdaLogExporter_SizeLimitIncludesNewline checks that the size limit
// counts the newline that ends the written line. With a limit equal to the
// full line length, the record is written whole. With a limit one byte
// lower, the record is truncated and the written line still fits.
func TestLambdaLogExporter_SizeLimitIncludesNewline(t *testing.T) {
	rec := byNameTestRecord()
	var whole bytes.Buffer
	if err := (&LambdaLogExporter{Writer: &whole}).Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lineLen := whole.Len()

	for _, tc := range []struct {
		name          string
		limit         int
		wantTruncated bool
	}{
		{name: "limit equals the line length", limit: lineLen, wantTruncated: false},
		{name: "limit is one byte lower", limit: lineLen - 1, wantTruncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			exp := &LambdaLogExporter{Writer: &buf, MaxSizeBytes: tc.limit}
			out := Truncate(rec, exp.MaxRecordSizeBytes(), exp.Render)
			if out.Truncated != tc.wantTruncated {
				t.Fatalf("Truncated = %v, want %v", out.Truncated, tc.wantTruncated)
			}
			if err := exp.Export(context.Background(), out); err != nil {
				t.Fatalf("Export: %v", err)
			}
			if buf.Len() > tc.limit {
				t.Errorf("wrote %d bytes, over the %d-byte limit", buf.Len(), tc.limit)
			}
			if !strings.HasSuffix(buf.String(), "\n") {
				t.Error("written line does not end with a newline")
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
