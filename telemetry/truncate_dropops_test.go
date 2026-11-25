package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// buildOversizedRecord returns a record with n operations whose rendered size
// comfortably exceeds maxBytes, so Truncate must drop whole operations. The
// operation IDs are "op-0" (oldest) to "op-<n-1>" (newest).
func buildOversizedRecord(n int) Record {
	ops := make([]OperationRecord, n)
	for i := range ops {
		ops[i] = OperationRecord{
			ID:     "op-" + strconv.Itoa(i),
			Type:   "step",
			Status: "SUCCEEDED",
			Result: json.RawMessage(`"` + strings.Repeat("y", 64) + `"`),
		}
	}
	return Record{
		RecordType:   "DurableExecutionTelemetry",
		ExecutionArn: "arn:test",
		Operations:   ops,
	}
}

// When clearing every result is not enough, Truncate drops whole
// operations, oldest first. It must keep the newest operations, and it must
// drop only as many as the limit requires.
func TestTruncate_DropOperations_KeepsNewestThatFit(t *testing.T) {
	const n = 500
	record := buildOversizedRecord(n)

	full, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// A cap that forces dropping the vast majority of operations.
	maxBytes := len(full) / 50

	out := Truncate(record, maxBytes, nil)

	// Must actually fit now, with the final metadata accounted for.
	if b, _ := json.Marshal(out); len(b) > maxBytes {
		t.Fatalf("result is %d bytes, exceeds cap %d", len(b), maxBytes)
	}
	dropped := out.DroppedOperations
	if !out.Truncated || dropped == 0 {
		t.Fatalf("expected Truncated with DroppedOperations>0, got Truncated=%v Dropped=%d",
			out.Truncated, dropped)
	}
	if len(out.Operations) == 0 {
		t.Fatalf("every operation was dropped; the cap %d leaves room for some", maxBytes)
	}
	if got := dropped + len(out.Operations); got != n {
		t.Fatalf("dropped(%d)+kept(%d) = %d, want %d", dropped, len(out.Operations), got, n)
	}

	// The kept operations are the newest ones, in order.
	for i, op := range out.Operations {
		if want := "op-" + strconv.Itoa(dropped+i); op.ID != want {
			t.Fatalf("kept operation %d has ID %q, want %q", i, op.ID, want)
		}
	}

	// Dropping one operation fewer would not fit. The extra operation is
	// the newest dropped one, in the state the result-clearing step left it.
	oneFewer := out
	extra := record.Operations[dropped-1]
	extra.Result = nil
	extra.Truncated = true
	oneFewer.Operations = append([]OperationRecord{extra}, out.Operations...)
	oneFewer.DroppedOperations = dropped - 1
	if b, _ := json.Marshal(oneFewer); len(b) <= maxBytes {
		t.Fatalf("dropping %d operations also fits (%d bytes, cap %d); Truncate dropped %d",
			dropped-1, len(b), maxBytes, dropped)
	}
}

// buildFailedRecord returns a record with n failed operations. Each
// operation has a unique name and an Error with a 200-byte message. This is
// the shape of the large-record report on PR #175.
func buildFailedRecord(n int) Record {
	ops := make([]OperationRecord, n)
	for i := range ops {
		ops[i] = OperationRecord{
			ID:     "op-" + strconv.Itoa(i),
			Name:   "step-" + strconv.Itoa(i),
			Type:   "STEP",
			Status: "FAILED",
			Error:  &ErrorRecord{Type: "TestError", Message: strings.Repeat("e", 200)},
		}
	}
	return Record{
		RecordType:   "DurableExecutionTelemetry",
		ExecutionArn: "arn:test",
		Operations:   ops,
	}
}

// dropOldestForTest returns record with its oldest k operations removed and
// the truncation metadata that Truncate sets in Step 2.
func dropOldestForTest(record Record, k int) Record {
	if k == 0 {
		return record
	}
	record.Operations = record.Operations[k:]
	record.Truncated = true
	record.DroppedOperations = k
	return record
}

// linearDropCount renders every count of removed operations, smallest
// first. It returns the first count that fits, or len(Operations) when
// none fits. It is the reference result for Step 2. It is valid only for a
// record without a Result, because then Step 1 changes nothing.
func linearDropCount(record Record, maxBytes int, render Renderer) int {
	for k := 0; k <= len(record.Operations); k++ {
		if fits(dropOldestForTest(record, k), maxBytes, render) {
			return k
		}
	}
	return len(record.Operations)
}

// Unique names give the record no count at which a removal makes it
// larger. So Step 2 needs only its binary search. The record has 2,000
// operations and is about 670 KB. Removing one operation at a time took
// about 1,800 renders for this record.
func TestTruncate_DropOperations_RenderCountIsLogarithmic(t *testing.T) {
	exp := &LambdaLogExporter{}
	renders := 0
	counting := func(r Record) ([]byte, error) {
		renders++
		return exp.Render(r)
	}

	out := Truncate(buildFailedRecord(2000), exp.MaxRecordSizeBytes(), counting)

	if out.DroppedOperations == 0 {
		t.Fatalf("DroppedOperations = 0; the record must be over the limit")
	}
	if renders > 40 {
		t.Fatalf("Truncate rendered the record %d times, want at most 40", renders)
	}

	// With unique names each removal makes the record smaller. So the
	// smallest count that fits is the one whose predecessor does not fit.
	record, limit, k := buildFailedRecord(2000), exp.MaxRecordSizeBytes(), out.DroppedOperations
	if !fits(dropOldestForTest(record, k), limit, exp.Render) {
		t.Fatalf("dropping %d operations does not fit", k)
	}
	if fits(dropOldestForTest(record, k-1), limit, exp.Render) {
		t.Fatalf("dropping %d operations also fits; Truncate dropped %d", k-1, k)
	}
}

// A binary search over the count of removed operations can return a count
// whose predecessor does not fit, while a smaller count fits. Truncate
// accepts this trade-off: it returns the count of the binary search, and
// the result still fits.
//
// The record has a unique operation "big" with a large Error. Then comes
// the older of two operations named "repeated". Then come small unique
// operations. The newest operation is the newer "repeated", with a large
// Error. The rendered size for each count is:
//
//   - 0: over the limit, because the Error of "big" is rendered.
//   - 1: exactly the limit. "repeated" occurs twice, so its Error is not
//     rendered.
//   - 2 to 18: over the limit. "repeated" occurs once, so its Error is
//     rendered.
//   - 19 and above: under the limit, because enough small operations are
//     gone.
//
// So the fewest drops that fit is 1, and a plain binary search returns
// more. Truncate must drop at least the fewest, and the result must fit.
func TestTruncate_DropOperations_ByNameCanDropMoreThanNeeded(t *testing.T) {
	failed := func(id, name string, messageLen int) OperationRecord {
		return OperationRecord{
			ID: id, Name: name, Type: "STEP", Status: "FAILED",
			Error: &ErrorRecord{Type: "TestError", Message: strings.Repeat("e", messageLen)},
		}
	}
	ops := []OperationRecord{failed("0", "big", 2000), failed("1", "repeated", 10)}
	for i := 2; i < 30; i++ {
		ops = append(ops, failed(strconv.Itoa(i), "small-"+strconv.Itoa(i), 10))
	}
	ops = append(ops, failed("30", "repeated", 2000))
	record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
	n := len(ops)

	exp := &LambdaLogExporter{}
	b, err := exp.Render(dropOldestForTest(record, 1))
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	limit := len(b)

	if fewest := linearDropCount(record, limit, exp.Render); fewest != 1 {
		t.Fatalf("fewest drops that fit = %d, want 1", fewest)
	}
	plain := 1 + sort.Search(n, func(i int) bool {
		return fits(dropOldestForTest(record, i+1), limit, exp.Render)
	})
	if plain == 1 {
		t.Fatalf("a plain binary search returns 1; the record does not show the trade-off")
	}

	out := Truncate(record, limit, exp.Render)

	if out.DroppedOperations < 1 {
		t.Fatalf("DroppedOperations = %d, below the fewest 1", out.DroppedOperations)
	}
	if !fits(out, limit, exp.Render) {
		t.Fatalf("result does not fit the %d-byte limit", limit)
	}
}

// When every removal makes the rendered record smaller, the binary search
// returns the fewest drops that fit. So Truncate must return the same count
// as a scan of every count. The array renderings meet this condition: each
// record has at most 9 operations, so DroppedOperations never gains a
// digit. The by-name renderings do not meet it. For them Truncate must
// return a count that is at least the scan result and fits. The records
// are generated from fixed seeds. They have repeated names, Errors of
// different sizes, and operations without a name. The limits are each
// candidate's rendered size and one byte less, because the result can
// change only at those limits.
func TestTruncate_DropOperations_MatchesLinearScan(t *testing.T) {
	renderers := []struct {
		name   string
		render Renderer
		exact  bool
	}{
		{"canonical", nil, true},
		{"array", func(r Record) ([]byte, error) { return RenderRecord(r, OperationsFormatArray) }, true},
		{"LambdaLogExporter", (&LambdaLogExporter{}).Render, false},
		{"CloudWatchLogsExporter", (&CloudWatchLogsExporter{}).Render, false},
	}
	names := []string{"a", "b", "c", ""}

	for _, r := range renderers {
		t.Run(r.name, func(t *testing.T) {
			render := r.render
			if render == nil {
				render = defaultRenderer
			}
			for seed := int64(0); seed < 300; seed++ {
				rng := rand.New(rand.NewSource(seed))
				ops := make([]OperationRecord, 1+rng.Intn(9))
				for i := range ops {
					ops[i] = OperationRecord{
						ID:     strconv.Itoa(i),
						Name:   names[rng.Intn(len(names))],
						Type:   "STEP",
						Status: "SUCCEEDED",
					}
					if rng.Intn(2) == 0 {
						ops[i].Status = "FAILED"
						ops[i].Error = &ErrorRecord{Type: "E", Message: strings.Repeat("e", rng.Intn(300))}
					}
				}
				record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}

				for k := 0; k <= len(ops); k++ {
					b, err := render(dropOldestForTest(record, k))
					if err != nil {
						t.Fatalf("Render: %v", err)
					}
					for _, limit := range []int{len(b), len(b) - 1} {
						want := linearDropCount(record, limit, render)
						out := Truncate(record, limit, r.render)
						got := out.DroppedOperations
						if r.exact && got != want {
							t.Fatalf("seed %d, limit %d: DroppedOperations = %d, want %d", seed, limit, got, want)
						}
						if got < want {
							t.Fatalf("seed %d, limit %d: DroppedOperations = %d, below the fewest %d", seed, limit, got, want)
						}
						if want < len(ops) && !fits(out, limit, render) {
							t.Fatalf("seed %d, limit %d: result does not fit", seed, limit)
						}
					}
				}
			}
		})
	}
}

// compactLen returns the length of result as the JSON encoder renders it.
func compactLen(t *testing.T, result json.RawMessage) int {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, result); err != nil {
		t.Fatalf("Compact(%s): %v", result, err)
	}
	return buf.Len()
}

// isCandidate reports whether Truncate may clear result. Clearing replaces
// `"result":<result>` (9 bytes plus the result) with `"truncated":true`
// (16 bytes). So only a result longer than 7 bytes makes the record
// smaller when cleared.
func isCandidate(t *testing.T, result json.RawMessage) bool {
	t.Helper()
	return len(result) > 0 && compactLen(t, result) > 7
}

// oneAtATimeTruncate is the reference result for Truncate. It renders after
// every single change. It clears results oldest first, then removes
// operations oldest first, then drops the input, then the output. It stops
// at the first record that fits. When clearSmall is false, it clears only
// results longer than 7 bytes, as Truncate does for the array rendering.
// When clearSmall is true, it clears every result, as Truncate does for
// the by-name rendering.
func oneAtATimeTruncate(t *testing.T, record Record, maxBytes int, render Renderer, clearSmall bool) Record {
	t.Helper()
	if fits(record, maxBytes, render) {
		return record
	}
	ops := slices.Clone(record.Operations)
	record.Operations = ops
	for i := range ops {
		if len(ops[i].Result) == 0 || !clearSmall && !isCandidate(t, ops[i].Result) {
			continue
		}
		ops[i].Result = nil
		ops[i].Truncated = true
		record.Truncated = true
		if fits(record, maxBytes, render) {
			return record
		}
	}
	for k := 1; k <= len(ops); k++ {
		if candidate := dropOldestForTest(record, k); fits(candidate, maxBytes, render) {
			return candidate
		}
	}
	if len(ops) > 0 {
		record = dropOldestForTest(record, len(ops))
	}
	if len(record.Input) > 0 {
		record.Input = nil
		record.Truncated = true
		record.DroppedInput = true
		if fits(record, maxBytes, render) {
			return record
		}
	}
	if len(record.Output) > 0 {
		record.Output = nil
		record.Truncated = true
		record.DroppedOutput = true
	}
	return record
}

// smallResults are results of 7 bytes or less as rendered. Clearing one of
// them makes the array rendering larger. `[ 1 , 2 ]` is 9 bytes as given
// and 5 bytes as rendered.
var smallResults = []string{`true`, `1`, `"ok"`, `null`, `""`, `{}`, `[ 1 , 2 ]`}

// For random records and limits, Truncate must return a record that fits
// whenever dropping every operation, the input and the output fits. It
// must remove only the oldest operations, and it must clear results longer
// than 7 bytes oldest first. The records use quotes, backslashes, non-ASCII
// text, repeated and empty names, short and long results, errors, input
// and output.
//
// The canonical and array renderings write a per-operation truncated
// marker. So there Truncate must keep every result of 7 bytes or less, and
// it must return the same record as oneAtATimeTruncate.
func TestTruncate_RandomRecordsFitAndDropOldestFirst(t *testing.T) {
	texts := []string{`"q"`, `\b\`, "é✓", "日本", "", "plain"}
	names := []string{"a", "a\\", `"b"`, "ü", ""}
	renderers := []struct {
		name   string
		render Renderer
		exact  bool
	}{
		{"canonical", defaultRenderer, true},
		{"array", func(r Record) ([]byte, error) { return RenderRecord(r, OperationsFormatArray) }, true},
		{"LambdaLogExporter", (&LambdaLogExporter{}).Render, false},
		{"both", func(r Record) ([]byte, error) { return RenderRecord(r, OperationsFormatBoth) }, false},
	}
	for _, r := range renderers {
		t.Run(r.name, func(t *testing.T) {
			for seed := int64(0); seed < 300; seed++ {
				rng := rand.New(rand.NewSource(seed))
				text := func() string {
					return strings.Repeat(texts[rng.Intn(len(texts))], 1+rng.Intn(40))
				}
				ops := make([]OperationRecord, rng.Intn(40))
				for i := range ops {
					ops[i] = OperationRecord{
						ID: strconv.Itoa(i), Name: names[rng.Intn(len(names))], Type: "STEP", Status: "SUCCEEDED",
					}
					switch rng.Intn(3) {
					case 1:
						ops[i].Result = json.RawMessage(smallResults[rng.Intn(len(smallResults))])
					case 2:
						ops[i].Result, _ = json.Marshal(text())
					}
					if rng.Intn(3) == 0 {
						ops[i].Status = "FAILED"
						ops[i].Error = &ErrorRecord{Type: "E", Message: text()}
					}
				}
				record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
				record.Input, _ = json.Marshal(text())
				record.Output, _ = json.Marshal(text())

				full, err := r.render(record)
				if err != nil {
					t.Fatalf("Render: %v", err)
				}
				empty := record
				empty.Operations, empty.Input, empty.Output = nil, nil, nil
				empty.Truncated, empty.DroppedInput, empty.DroppedOutput = true, true, true
				empty.DroppedOperations = len(ops)
				floor, err := r.render(empty)
				if err != nil {
					t.Fatalf("Render: %v", err)
				}
				limit := len(floor) + rng.Intn(len(full)-len(floor)+1)

				out := Truncate(record, limit, r.render)

				if !fits(out, limit, r.render) {
					t.Fatalf("seed %d, limit %d: result does not fit", seed, limit)
				}
				k := out.DroppedOperations
				if k+len(out.Operations) != len(ops) {
					t.Fatalf("seed %d: dropped %d + kept %d != %d", seed, k, len(out.Operations), len(ops))
				}
				kept := false
				for i, op := range out.Operations {
					orig := ops[k+i]
					if op.ID != orig.ID {
						t.Fatalf("seed %d: kept operation %d has ID %q, want %q", seed, i, op.ID, orig.ID)
					}
					if len(orig.Result) == 0 {
						continue
					}
					if !isCandidate(t, orig.Result) {
						if r.exact && (string(op.Result) != string(orig.Result) || op.Truncated) {
							t.Fatalf("seed %d: operation %s lost its %d-byte result %s", seed, op.ID, compactLen(t, orig.Result), orig.Result)
						}
						continue
					}
					if len(op.Result) > 0 {
						kept = true
					} else if kept {
						t.Fatalf("seed %d: operation %s lost its result, but an older one kept its result", seed, op.ID)
					}
				}
				if r.exact {
					if want := oneAtATimeTruncate(t, record, limit, r.render, false); !reflect.DeepEqual(out, want) {
						t.Fatalf("seed %d, limit %d: Truncate = %+v, want the one-at-a-time result %+v", seed, limit, out, want)
					}
				}
			}
		})
	}
}

// One large old result is followed by many results of 7 bytes or less.
// The limit is one byte below the full size. Clearing the large result is
// enough. Clearing a small result makes the array rendering larger, so a
// search that also cleared the small results would find that clearing
// half or all of them does not fit, and would drop operations. Truncate
// must clear only the large result and drop no operation, as the
// one-at-a-time reference does.
func TestTruncate_Results_SkipsSmallResults(t *testing.T) {
	small := []string{`true`, `1`, `"ok"`, `null`}
	ops := []OperationRecord{{
		ID: "op-0", Type: "STEP", Status: "SUCCEEDED",
		Result: json.RawMessage(`"` + strings.Repeat("x", 1000) + `"`),
	}}
	for i := 1; i < 2000; i++ {
		ops = append(ops, OperationRecord{
			ID: "op-" + strconv.Itoa(i), Type: "STEP", Status: "SUCCEEDED",
			Result: json.RawMessage(small[i%len(small)]),
		})
	}
	record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
	render := func(r Record) ([]byte, error) { return RenderRecord(r, OperationsFormatArray) }
	full, err := render(record)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	limit := len(full) - 1

	out := Truncate(record, limit, render)

	if want := oneAtATimeTruncate(t, record, limit, render, false); !reflect.DeepEqual(out, want) {
		t.Fatalf("Truncate differs from the one-at-a-time result: dropped %d, want %d",
			out.DroppedOperations, want.DroppedOperations)
	}
	if out.DroppedOperations != 0 || len(out.Operations) != len(ops) {
		t.Fatalf("DroppedOperations = %d, want 0", out.DroppedOperations)
	}
	if out.Operations[0].Result != nil || !out.Operations[0].Truncated {
		t.Fatalf("the large result was not cleared")
	}
	for _, op := range out.Operations[1:] {
		if op.Result == nil || op.Truncated {
			t.Fatalf("operation %s lost its small result", op.ID)
		}
	}
}

// The by-name rendering writes no per-operation truncated marker. So
// there clearing a small result makes the record smaller. One large old
// result is followed by many small results, each under a unique name. The
// limit is the size after the large result and the oldest 500 small
// results are cleared. Truncate must clear small results too and drop no
// operation, as the one-at-a-time reference that clears every result does.
func TestTruncate_Results_ByNameClearsSmallResults(t *testing.T) {
	small := []string{`true`, `1`, `"ok"`, `null`}
	ops := []OperationRecord{{
		ID: "op-0", Name: "s-0", Type: "STEP", Status: "SUCCEEDED",
		Result: json.RawMessage(`"` + strings.Repeat("x", 1000) + `"`),
	}}
	for i := 1; i < 2000; i++ {
		ops = append(ops, OperationRecord{
			ID: "op-" + strconv.Itoa(i), Name: "s-" + strconv.Itoa(i), Type: "STEP", Status: "SUCCEEDED",
			Result: json.RawMessage(small[i%len(small)]),
		})
	}
	record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
	render := (&LambdaLogExporter{}).Render
	cleared := record
	cleared.Operations = slices.Clone(ops)
	for i := 0; i <= 500; i++ {
		cleared.Operations[i].Result = nil
		cleared.Operations[i].Truncated = true
	}
	cleared.Truncated = true
	b, err := render(cleared)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	limit := len(b)

	out := Truncate(record, limit, render)

	want := oneAtATimeTruncate(t, record, limit, render, true)
	if want.DroppedOperations != 0 {
		t.Fatalf("the one-at-a-time reference dropped %d operations, want 0", want.DroppedOperations)
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("Truncate differs from the one-at-a-time result: dropped %d, want %d",
			out.DroppedOperations, want.DroppedOperations)
	}
}

// The record fits only when every candidate result is cleared. Truncate
// must clear all of them and drop no operation. The small results stay.
func TestTruncate_Results_ClearingEveryCandidateFits(t *testing.T) {
	var ops []OperationRecord
	for i := 0; i < 10; i++ {
		ops = append(ops, OperationRecord{
			ID: "op-" + strconv.Itoa(i), Type: "STEP", Status: "SUCCEEDED",
			Result: json.RawMessage(`"` + strings.Repeat("y", 64) + `"`),
		})
		ops = append(ops, OperationRecord{
			ID: "small-" + strconv.Itoa(i), Type: "STEP", Status: "SUCCEEDED", Result: json.RawMessage(`true`),
		})
	}
	record := Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
	cleared := record
	cleared.Operations = slices.Clone(ops)
	for i := 0; i < len(ops); i += 2 {
		cleared.Operations[i].Result = nil
		cleared.Operations[i].Truncated = true
	}
	cleared.Truncated = true
	b, err := marshalNoHTMLEscape(cleared)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	out := Truncate(record, len(b), nil)

	if !reflect.DeepEqual(out, cleared) {
		t.Fatalf("Truncate = %+v, want every candidate result cleared and no operation dropped", out)
	}
}

// The record fits only when every operation is dropped. Truncate must drop
// all of them and keep the input.
func TestTruncate_DropOperations_DroppingEveryOperationFits(t *testing.T) {
	record := buildFailedRecord(20)
	record.Input = json.RawMessage(`"` + strings.Repeat("i", 100) + `"`)
	allDropped := dropOldestForTest(record, len(record.Operations))
	b, err := marshalNoHTMLEscape(allDropped)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	out := Truncate(record, len(b), nil)

	if out.DroppedOperations != 20 || len(out.Operations) != 0 {
		t.Fatalf("DroppedOperations = %d with %d kept, want 20 dropped", out.DroppedOperations, len(out.Operations))
	}
	if out.DroppedInput || len(out.Input) == 0 {
		t.Fatalf("the input was dropped, but the record fits with it")
	}
}

// A custom Renderer must get the same bounded render count as a built-in
// exporter. The renderer wraps the canonical JSON in an envelope.
func TestTruncate_CustomRendererRenderCountIsLogarithmic(t *testing.T) {
	renders := 0
	custom := func(r Record) ([]byte, error) {
		renders++
		b, err := marshalNoHTMLEscape(r)
		if err != nil {
			return nil, err
		}
		return append(append([]byte(`{"envelope":`), b...), '}'), nil
	}

	for _, record := range []Record{buildOversizedRecord(2000), buildTwiceWithErrorRecord(2000)} {
		renders = 0
		out := Truncate(record, 100000, custom)
		if out.DroppedOperations == 0 {
			t.Fatalf("DroppedOperations = 0; the record must be over the limit")
		}
		if renders >= 40 {
			t.Fatalf("Truncate rendered the record %d times, want fewer than 40", renders)
		}
	}
}

// BenchmarkTruncate_DropOperations sizes records of the shape in the
// large-record report on PR #175 against the default LambdaLogExporter
// limit.
func BenchmarkTruncate_DropOperations(b *testing.B) {
	for _, n := range []int{2000, 8000} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			exp := &LambdaLogExporter{}
			record := buildFailedRecord(n)
			for b.Loop() {
				Truncate(record, exp.MaxRecordSizeBytes(), exp.Render)
			}
		})
	}
}

// buildTwiceWithErrorRecord returns a record with n operations in which
// every name occurs exactly twice. The older operation of each name comes
// first and has no content. The newer one has an Error with a 200-byte
// message. Each name is 200 bytes long, so the by-name rendering is over
// the LambdaLogExporter limit at n = 2,000.
func buildTwiceWithErrorRecord(n int) Record {
	half := n / 2
	ops := make([]OperationRecord, 0, n)
	name := func(i int) string { return fmt.Sprintf("step-%04d-", i) + strings.Repeat("n", 190) }
	for i := 0; i < half; i++ {
		ops = append(ops, OperationRecord{
			ID: "op-" + strconv.Itoa(i), Name: name(i), Type: "STEP", Status: "SUCCEEDED",
		})
	}
	for i := 0; i < half; i++ {
		ops = append(ops, OperationRecord{
			ID: "op-" + strconv.Itoa(half+i), Name: name(i), Type: "STEP", Status: "FAILED",
			Error: &ErrorRecord{Type: "TestError", Message: strings.Repeat("e", 200)},
		})
	}
	return Record{RecordType: "DurableExecutionTelemetry", ExecutionArn: "arn:test", Operations: ops}
}

// Every name occurs twice and the newer operation has an Error. Each
// removal of an older operation makes one more Error appear in the
// by-name rendering. The render count must still stay logarithmic.
func TestTruncate_DropOperations_TwiceWithErrorRenderCountIsLogarithmic(t *testing.T) {
	exp := &LambdaLogExporter{}
	renders := 0
	counting := func(r Record) ([]byte, error) {
		renders++
		return exp.Render(r)
	}

	record := buildTwiceWithErrorRecord(2000)
	out := Truncate(record, exp.MaxRecordSizeBytes(), counting)

	if out.DroppedOperations == 0 {
		t.Fatalf("DroppedOperations = 0; the record must be over the limit")
	}
	if !fits(out, exp.MaxRecordSizeBytes(), exp.Render) {
		t.Fatalf("result does not fit the limit")
	}
	if renders >= 40 {
		t.Fatalf("Truncate rendered the record %d times, want fewer than 40", renders)
	}
}
