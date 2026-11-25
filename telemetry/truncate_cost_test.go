package telemetry

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// truncateForExporter applies the truncation that dispatch applies to
// exp. It sizes with render instead of exp.Render, so a test can count the
// renders.
func truncateForExporter(record Record, exp RenderingSizeLimiter, render Renderer) Record {
	return truncate(record, exp.MaxRecordSizeBytes(), render, meteredCopies(exp))
}

// buildSimpleNamedRecord returns n succeeded steps with unique names,
// timestamps and durations, and no result or error.
func buildSimpleNamedRecord(n int) Record {
	start := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	ops := make([]OperationRecord, n)
	for i := range ops {
		at := start.Add(time.Duration(i) * 10 * time.Millisecond)
		end := at.Add(3 * time.Millisecond)
		duration := int64(3)
		ops[i] = OperationRecord{
			ID: "op-" + strconv.Itoa(i), Name: "step-" + strconv.Itoa(i),
			Type: "STEP", SubType: "Step", Status: "SUCCEEDED",
			StartTimestamp: &at, EndTimestamp: &end, DurationMs: &duration, Attempt: 1,
		}
	}
	return Record{
		RecordType:   "DurableExecutionTelemetry",
		ExecutionArn: "arn:aws:lambda:us-east-1:123456789012:function:demo:$LATEST/durable-execution/run-1/inv-1",
		Operations:   ops,
	}
}

// buildTwiceNamedRecord returns n operations in which every name occurs
// twice: operation i and operation i+n/2. The newer operation has an
// Error. In operationsByName a name that occurs twice shows no Error. So
// dropping the older operation of a pair makes the newer one's Error
// appear, and the rendered record grows.
func buildTwiceNamedRecord(n int) Record {
	half := n / 2
	ops := make([]OperationRecord, n)
	for i := range ops {
		ops[i] = OperationRecord{
			ID:     "op-" + strconv.Itoa(i),
			Name:   "step-" + strings.Repeat("n", 260) + strconv.Itoa(i%half),
			Type:   "STEP",
			Status: "SUCCEEDED",
		}
		if i >= half {
			ops[i].Status = "FAILED"
			ops[i].Error = &ErrorRecord{Type: "TestError", Message: strings.Repeat("e", 200)}
		}
	}
	return Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
}

// A built-in exporter must size a large record a bounded number of times.
// Rendering once per dropped operation is quadratic in the record size.
func TestTruncate_BuiltInExporterRenderCount(t *testing.T) {
	tests := []struct {
		name   string
		exp    RenderingSizeLimiter
		record Record
	}{
		{"simple named steps, by-name", &CloudWatchLogsExporter{MaxSizeBytes: 256_000}, buildSimpleNamedRecord(2000)},
		{"every name twice, by-name", &LambdaLogExporter{}, buildTwiceNamedRecord(2000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			renders := 0
			counting := func(r Record) ([]byte, error) {
				renders++
				return tt.exp.Render(r)
			}
			out := truncateForExporter(tt.record, tt.exp, counting)
			if out.DroppedOperations == 0 {
				t.Fatalf("DroppedOperations = 0; the record must be over the limit")
			}
			if renders >= 150 {
				t.Fatalf("truncation rendered the record %d times, want fewer than 150", renders)
			}
		})
	}
}

// The meter must not change the result. For each built-in rendering, the
// metered path and the exact path must return the same record. The
// records are generated from fixed seeds. Their names, results, errors,
// input and output contain quotes, backslashes, non-ASCII text, emoji,
// control characters and HTML characters. Names repeat, and some are
// empty. The limits are spread from just under the full size down to
// below the size with every operation removed.
func TestTruncate_MeteredMatchesExact(t *testing.T) {
	exporters := []struct {
		name string
		exp  RenderingSizeLimiter
	}{
		{"LambdaLogExporter", &LambdaLogExporter{}},
		{"CloudWatchLogsExporter", &CloudWatchLogsExporter{}},
		{"SQSExporter array", &SQSExporter{}},
		{"SQSExporter by-name", &SQSExporter{OperationsFormat: OperationsFormatByName}},
		{"SQSExporter both", &SQSExporter{OperationsFormat: OperationsFormatBoth}},
		{"EventBridgeExporter both", &EventBridgeExporter{OperationsFormat: OperationsFormatBoth}},
		{"FirehoseExporter both", &FirehoseExporter{OperationsFormat: OperationsFormatBoth}},
	}
	texts := []string{"", "a", `q"uote`, `back\slash`, "über", "日本語", "😀🎉", "ctl\x01\x1f\n\t", "<&>", strings.Repeat("x", 120)}
	text := func(rng *rand.Rand) string { return texts[rng.Intn(len(texts))] + texts[rng.Intn(len(texts))] }
	value := func(rng *rand.Rand) json.RawMessage {
		b, err := marshalNoHTMLEscape(map[string]any{"v": text(rng), "n": rng.Intn(1000)})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}
	names := []string{"", "a", "b", `"c\`, "😀", "ünï"}

	for _, e := range exporters {
		t.Run(e.name, func(t *testing.T) {
			copies := meteredCopies(e.exp)
			if copies == 0 {
				t.Fatalf("meteredCopies = 0 for a built-in exporter")
			}
			meteredRenders, exactRenders := 0, 0
			for seed := int64(0); seed < 20; seed++ {
				rng := rand.New(rand.NewSource(seed))
				ops := make([]OperationRecord, rng.Intn(20))
				for i := range ops {
					duration := int64(rng.Intn(100_000))
					ops[i] = OperationRecord{
						ID: strconv.Itoa(i), Name: names[rng.Intn(len(names))],
						Type: "STEP", Status: "SUCCEEDED", DurationMs: &duration, Attempt: rng.Intn(3),
					}
					if rng.Intn(2) == 0 {
						ops[i].Result = value(rng)
					}
					if rng.Intn(3) == 0 {
						ops[i].Status = "FAILED"
						ops[i].Error = &ErrorRecord{Type: text(rng), Message: text(rng), StackTrace: text(rng)}
					}
				}
				record := Record{
					RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test",
					Operations: ops, Input: value(rng), Output: value(rng),
				}
				full, err := e.exp.Render(record)
				if err != nil {
					t.Fatalf("Render: %v", err)
				}
				for limit := len(full); limit > 0; limit -= 1 + rng.Intn(len(full)/8+1) {
					metered := truncate(record, limit, counter(&meteredRenders, e.exp.Render), copies)
					exact := truncate(record, limit, counter(&exactRenders, e.exp.Render), 0)
					if !reflect.DeepEqual(metered, exact) {
						t.Fatalf("seed %d, limit %d: metered DroppedOperations=%d, exact %d",
							seed, limit, metered.DroppedOperations, exact.DroppedOperations)
					}
				}
			}
			// The meter must skip checks, or this test does not exercise it.
			if meteredRenders >= exactRenders {
				t.Fatalf("metered path rendered %d times, exact path %d", meteredRenders, exactRenders)
			}
		})
	}
}

// counter returns render, counting its calls in n.
func counter(n *int, render Renderer) Renderer {
	return func(r Record) ([]byte, error) {
		*n++
		return render(r)
	}
}

// overriddenRender embeds a built-in exporter and defines its own Render.
type overriddenRender struct {
	*LambdaLogExporter
}

func (overriddenRender) Render(Record) ([]byte, error) { return []byte("{}"), nil }

// Only this package's exporters, by exact type, use the meter. Any other
// exporter, including one that embeds a built-in exporter, gets the exact
// path, because the meter's bound holds only for a known rendering.
func TestMeteredCopies(t *testing.T) {
	tests := []struct {
		name string
		exp  RenderingSizeLimiter
		want int
	}{
		{"LambdaLogExporter", &LambdaLogExporter{}, 1},
		{"CloudWatchLogsExporter", &CloudWatchLogsExporter{}, 1},
		{"SQSExporter default", &SQSExporter{}, 1},
		{"SQSExporter by-name", &SQSExporter{OperationsFormat: OperationsFormatByName}, 1},
		{"SQSExporter both", &SQSExporter{OperationsFormat: OperationsFormatBoth}, 2},
		{"EventBridgeExporter both", &EventBridgeExporter{OperationsFormat: OperationsFormatBoth}, 2},
		{"FirehoseExporter both", &FirehoseExporter{OperationsFormat: OperationsFormatBoth}, 2},
		{"embedded built-in", overriddenRender{&LambdaLogExporter{}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := meteredCopies(tt.exp); got != tt.want {
				t.Fatalf("meteredCopies = %d, want %d", got, tt.want)
			}
		})
	}
}

// A custom Render passed to Truncate is measured after every step. The
// record has no result, so every render belongs to Step 2. Each step
// renders once, so the count is one more than the dropped operations.
func TestTruncate_CustomRenderUsesExactPath(t *testing.T) {
	exp := &LambdaLogExporter{}
	record := buildFailedRecord(300)
	renders := 0
	out := Truncate(record, 20_000, func(r Record) ([]byte, error) {
		renders++
		return exp.Render(r)
	})
	if out.DroppedOperations == 0 {
		t.Fatalf("DroppedOperations = 0; the record must be over the limit")
	}
	if want := out.DroppedOperations + 1; renders != want {
		t.Fatalf("renders = %d, want %d", renders, want)
	}
}

// OperationsFormatBoth renders each operation twice. A bound that covered
// one copy would be too small when each operation carries a large Error:
// the meter would skip a check that fits, and truncation would drop extra
// operations.
func TestTruncate_MeteredMatchesExact_BothLargeErrors(t *testing.T) {
	exp := &SQSExporter{OperationsFormat: OperationsFormatBoth}
	ops := make([]OperationRecord, 30)
	for i := range ops {
		ops[i] = OperationRecord{
			ID: strconv.Itoa(i), Name: "step-" + strconv.Itoa(i), Type: "STEP", Status: "FAILED",
			Error: &ErrorRecord{Type: "E", Message: strings.Repeat("e", 3000)},
		}
	}
	record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
	full, err := exp.Render(record)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	for limit := len(full) - 1; limit > 0; limit -= 997 {
		metered := truncate(record, limit, exp.Render, meteredCopies(exp))
		exact := truncate(record, limit, exp.Render, 0)
		if metered.DroppedOperations != exact.DroppedOperations {
			t.Fatalf("limit %d: metered DroppedOperations=%d, exact %d",
				limit, metered.DroppedOperations, exact.DroppedOperations)
		}
	}
}
