package telemetry

import (
	"encoding/json"
	"fmt"
	"math/rand"
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

// Unique names make each removal shrink the record. The record has 2,000
// operations and is about 670 KB. Removing one operation at a time and
// rendering after each took about 1,800 renders for this record. The
// LambdaLogExporter uses the meter, so it renders far fewer times. The
// result is still the smallest count that fits.
func TestTruncate_DropOperations_RenderCountIsBounded(t *testing.T) {
	exp := &LambdaLogExporter{}
	renders := 0
	counting := func(r Record) ([]byte, error) {
		renders++
		return exp.Render(r)
	}

	out := truncateForExporter(buildFailedRecord(2000), exp, counting)

	if out.DroppedOperations == 0 {
		t.Fatalf("DroppedOperations = 0; the record must be over the limit")
	}
	if renders > 40 {
		t.Fatalf("truncation rendered the record %d times, want at most 40", renders)
	}

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
// must still return the smallest count.
//
// The record has a unique operation "big" with a large Error. Then comes
// the older of two operations named "repeated". Then come small unique
// operations. The newest operation is the newer "repeated", with a large
// Error. The rendered size for each count is:
//
//   - 0: over the limit, because the Error of "big" is rendered.
//   - 1: exactly the limit. "repeated" occurs twice, so its Error is not
//     rendered.
//   - 2 up to n-1: over the limit. "repeated" occurs once, so its Error is
//     rendered.
//   - n: under the limit. No operation is left.
//
// So the correct answer is 1.
func TestTruncate_DropOperations_FindsCountBelowBinarySearchResult(t *testing.T) {
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

	// The test is meaningful only when a plain binary search misses 1.
	plain := 1 + sort.Search(n, func(i int) bool {
		return fits(dropOldestForTest(record, i+1), limit, exp.Render)
	})
	if plain == 1 {
		t.Fatalf("a plain binary search already returns 1; the record does not exercise the scan")
	}

	out := Truncate(record, limit, exp.Render)

	if out.DroppedOperations != 1 {
		t.Fatalf("DroppedOperations = %d, want 1 (a plain binary search returns %d)", out.DroppedOperations, plain)
	}
}

// Truncate must return the same count as a scan of every count. The
// records are generated from fixed seeds. They have repeated names, Errors
// of different sizes, and operations without a name. Each record has at
// most 9 operations, so DroppedOperations never gains a digit. No
// operation has a DurationMs, so no duration gains a digit. So the scan
// result is exact. The limits are each candidate's rendered size and one
// byte less, because the result can change only at those limits.
func TestTruncate_DropOperations_MatchesLinearScan(t *testing.T) {
	renderers := []struct {
		name   string
		render Renderer
	}{
		{"canonical", nil},
		{"LambdaLogExporter", (&LambdaLogExporter{}).Render},
		{"CloudWatchLogsExporter", (&CloudWatchLogsExporter{}).Render},
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
						if out.DroppedOperations != want {
							t.Fatalf("seed %d, limit %d: DroppedOperations = %d, want %d",
								seed, limit, out.DroppedOperations, want)
						}
					}
				}
			}
		})
	}
}

// BenchmarkTruncate_DropOperations sizes records of the shape in the
// large-record report on PR #175 against the default LambdaLogExporter
// limit, as dispatch does.
func BenchmarkTruncate_DropOperations(b *testing.B) {
	for _, n := range []int{2000, 8000} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			exp := &LambdaLogExporter{}
			record := buildFailedRecord(n)
			for b.Loop() {
				truncateForExporter(record, exp, exp.Render)
			}
		})
	}
}
