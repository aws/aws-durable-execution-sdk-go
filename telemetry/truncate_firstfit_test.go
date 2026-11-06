package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// nonMonoRenderer returns a size that is not monotonic in the number of
// dropped results or operations. Dropping exactly one oldest item fits;
// dropping a second item grows past the cap. Binary search skips the
// first fitting candidate and over-drops.
type nonMonoRenderer struct {
	phase     string // "result" or "operation"
	origCount int
}

func (n nonMonoRenderer) render(r Record) ([]byte, error) {
	extra := 0
	if r.Truncated {
		extra += 32
	}
	extra += 8 * r.DroppedOperations
	if r.DroppedInput {
		extra += 16
	}
	if r.DroppedOutput {
		extra += 16
	}

	switch n.phase {
	case "result":
		remaining := 0
		for _, op := range r.Operations {
			if op.Result != nil {
				remaining++
			}
		}
		// Original: 3 results. First-fit is remaining==2 (dropped oldest Result).
		if remaining == n.origCount-1 && r.DroppedOperations == 0 {
			return make([]byte, 40+extra), nil
		}
		return make([]byte, 400+extra), nil
	case "operation":
		if len(r.Operations) == n.origCount-1 {
			return make([]byte, 40+extra), nil
		}
		return make([]byte, 400+extra), nil
	default:
		return nil, fmt.Errorf("unknown phase %q", n.phase)
	}
}

func threeOpRecord(withResults bool) Record {
	ops := []OperationRecord{
		{ID: "op-oldest", Name: "a", Type: "step", Status: "SUCCEEDED"},
		{ID: "op-middle", Name: "b", Type: "step", Status: "SUCCEEDED"},
		{ID: "op-newest", Name: "c", Type: "step", Status: "SUCCEEDED"},
	}
	if withResults {
		for i := range ops {
			ops[i].Result = &ContentField{Value: fmt.Sprintf("result-%d", i)}
		}
	}
	return Record{
		RecordType:   RecordType,
		ExecutionArn: "arn:test",
		Operations:   ops,
	}
}

func TestTruncate_CustomRenderer_FirstFitResultDrop(t *testing.T) {
	record := threeOpRecord(true)
	src := snapshotOperations(record.Operations)
	r := nonMonoRenderer{phase: "result", origCount: 3}
	const maxBytes = 80

	out := Truncate(record, maxBytes, r.render)

	if !out.Truncated {
		t.Fatal("expected Truncated=true")
	}
	if out.DroppedOperations != 0 {
		t.Fatalf("DroppedOperations=%d, want 0 (result-drop phase)", out.DroppedOperations)
	}
	if len(out.Operations) != 3 {
		t.Fatalf("kept %d operations, want 3", len(out.Operations))
	}
	if got := opIDs(out.Operations); strings.Join(got, ",") != "op-oldest,op-middle,op-newest" {
		t.Fatalf("order=%v, want original order", got)
	}
	if out.Operations[0].Result != nil {
		t.Fatal("expected oldest Result to be dropped")
	}
	if !out.Operations[0].Truncated {
		t.Fatal("expected oldest operation Truncated=true")
	}
	if out.Operations[1].Result == nil || out.Operations[2].Result == nil {
		t.Fatal("expected newer Results to be retained")
	}

	b, err := r.render(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > maxBytes {
		t.Fatalf("rendered size %d exceeds cap %d (skipped first fit)", len(b), maxBytes)
	}

	assertOperationsUnchanged(t, record.Operations, src)
}

func TestTruncate_CustomRenderer_FirstFitOperationDrop(t *testing.T) {
	record := threeOpRecord(false)
	src := snapshotOperations(record.Operations)
	r := nonMonoRenderer{phase: "operation", origCount: 3}
	const maxBytes = 80

	out := Truncate(record, maxBytes, r.render)

	if !out.Truncated {
		t.Fatal("expected Truncated=true")
	}
	if out.DroppedOperations != 1 {
		t.Fatalf("DroppedOperations=%d, want 1 (first fitting whole-op drop)", out.DroppedOperations)
	}
	if len(out.Operations) != 2 {
		t.Fatalf("kept %d operations, want 2", len(out.Operations))
	}
	if got := opIDs(out.Operations); strings.Join(got, ",") != "op-middle,op-newest" {
		t.Fatalf("kept %v, want middle then newest (oldest-first drop, original order)", got)
	}

	b, err := r.render(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > maxBytes {
		t.Fatalf("rendered size %d exceeds cap %d (skipped first fit)", len(b), maxBytes)
	}

	assertOperationsUnchanged(t, record.Operations, src)
}

func TestTruncate_DuplicateName_ByNameAndBoth_FirstFit(t *testing.T) {
	formats := []OperationsFormat{OperationsFormatByName, OperationsFormatBoth}
	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			record := duplicateNameRecord()
			src := snapshotOperations(record.Operations)
			render := func(r Record) ([]byte, error) {
				return RenderRecord(r, format)
			}

			sizes := operationDropSizes(t, record, render)
			if sizes[1] >= sizes[0] {
				t.Fatalf("dropping the unique oldest op did not shrink: sizes=%v", sizes)
			}
			if sizes[2] <= sizes[1] {
				t.Fatalf("exposing the singleton error did not grow: sizes=%v", sizes)
			}

			maxBytes := sizes[1] // first fitting candidate, inclusive
			if sizes[0] <= maxBytes || sizes[2] <= maxBytes {
				t.Fatalf("cap %d does not isolate k=1: sizes=%v", maxBytes, sizes)
			}

			out := Truncate(record, maxBytes, render)

			if !out.Truncated {
				t.Fatal("expected Truncated=true")
			}
			if out.DroppedOperations != 1 {
				t.Fatalf("DroppedOperations=%d, want 1 (binary search skips this first fit)", out.DroppedOperations)
			}
			if got := opIDs(out.Operations); strings.Join(got, ",") != "op-shared-1,op-shared-2" {
				t.Fatalf("kept %v, want the two shared-name ops in original order", got)
			}

			b, err := render(out)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) > maxBytes {
				t.Fatalf("rendered size %d exceeds cap %d", len(b), maxBytes)
			}

			assertOperationsUnchanged(t, record.Operations, src)
		})
	}
}

func TestTruncate_DoesNotMutateSourceOperations(t *testing.T) {
	record := duplicateNameRecord()
	// Distinct backing-array contents, including pointers, so a write
	// through a shared array is visible.
	srcJSON, err := json.Marshal(record.Operations)
	if err != nil {
		t.Fatal(err)
	}
	srcHeader := record.Operations

	render := func(r Record) ([]byte, error) {
		return RenderRecord(r, OperationsFormatByName)
	}
	sizes := operationDropSizes(t, record, render)
	maxBytes := sizes[1]

	out := Truncate(record, maxBytes, render)
	if out.DroppedOperations == 0 && !out.Truncated {
		t.Fatal("expected truncation so the copy path is exercised")
	}

	gotJSON, err := json.Marshal(record.Operations)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(srcJSON, gotJSON) {
		t.Fatalf("source operations mutated:\n before=%s\n after=%s", srcJSON, gotJSON)
	}
	if len(record.Operations) != 3 {
		t.Fatalf("source Operations length=%d, want 3", len(record.Operations))
	}
	// Mutating the returned slice must not write through to the source.
	if len(out.Operations) == 0 {
		t.Fatal("expected some operations to remain")
	}
	out.Operations[0].ID = "mutated-in-output"
	if srcHeader[0].ID == "mutated-in-output" {
		t.Fatal("returned operations alias the source backing array")
	}
}

func duplicateNameRecord() Record {
	return Record{
		RecordType:   RecordType,
		ExecutionArn: "arn:test",
		Operations: []OperationRecord{
			{
				ID:     "op-unique",
				Name:   "unique",
				Type:   "step",
				Status: "FAILED",
				Error:  &ErrorRecord{Type: "UniqueErr", Message: strings.Repeat("U", 400)},
			},
			{
				ID:     "op-shared-1",
				Name:   "shared",
				Type:   "step",
				Status: "SUCCEEDED",
			},
			{
				ID:     "op-shared-2",
				Name:   "shared",
				Type:   "step",
				Status: "FAILED",
				Error:  &ErrorRecord{Type: "SharedErr", Message: strings.Repeat("E", 2000)},
			},
		},
	}
}

func operationDropSizes(t *testing.T, rec Record, render Renderer) []int {
	t.Helper()
	sizes := make([]int, len(rec.Operations)+1)
	for k := 0; k <= len(rec.Operations); k++ {
		c := rec
		ops := make([]OperationRecord, len(rec.Operations)-k)
		copy(ops, rec.Operations[k:])
		c.Operations = ops
		if k > 0 {
			c.Truncated = true
			c.DroppedOperations = k
		}
		b, err := render(c)
		if err != nil {
			t.Fatalf("render k=%d: %v", k, err)
		}
		sizes[k] = len(b)
	}
	return sizes
}

func snapshotOperations(ops []OperationRecord) []OperationRecord {
	out := make([]OperationRecord, len(ops))
	copy(out, ops)
	return out
}

func assertOperationsUnchanged(t *testing.T, got, want []OperationRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("source Operations length=%d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID {
			t.Errorf("source op[%d].ID=%q, want %q", i, got[i].ID, want[i].ID)
		}
		if got[i].Truncated != want[i].Truncated {
			t.Errorf("source op[%d].Truncated=%v, want %v", i, got[i].Truncated, want[i].Truncated)
		}
		if got[i].Result != want[i].Result {
			t.Errorf("source op[%d].Result pointer changed", i)
		}
		if got[i].Error != want[i].Error {
			t.Errorf("source op[%d].Error pointer changed", i)
		}
	}
}

func opIDs(ops []OperationRecord) []string {
	ids := make([]string, len(ops))
	for i, op := range ops {
		ids[i] = op.ID
	}
	return ids
}
