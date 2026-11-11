package telemetry

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func TestOrderedOperationIDs(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		ops  map[string]durable.OperationHookInfo
		want []string
	}{
		{name: "empty"},
		{
			name: "timestamp before ID",
			ops: map[string]durable.OperationHookInfo{
				"a": {StartTimestamp: start.Add(time.Second)},
				"b": {StartTimestamp: start},
			},
			want: []string{"b", "a"},
		},
		{
			name: "equal timestamps use ID",
			ops: map[string]durable.OperationHookInfo{
				"b": {StartTimestamp: start},
				"a": {StartTimestamp: start},
			},
			want: []string{"a", "b"},
		},
		{
			name: "unset timestamps last",
			ops: map[string]durable.OperationHookInfo{
				"a": {},
				"b": {},
				"c": {StartTimestamp: start},
			},
			want: []string{"c", "a", "b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := orderedOperationIDs(tc.ops); !slices.Equal(got, tc.want) {
				t.Errorf("orderedOperationIDs = %v, want %v", got, tc.want)
			}
		})
	}
}

type resumeLimitedExporter struct {
	capturingExporter
	maxBytes int
}

func (e *resumeLimitedExporter) MaxRecordSizeBytes() int { return e.maxBytes }

func TestResume_SizeLimitedExportDropsOldestFirst(t *testing.T) {
	for _, dropResults := range []bool{true, false} {
		name := "operations"
		if dropResults {
			name = "results"
		}
		t.Run(name, func(t *testing.T) {
			const count = 8
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			ops := make(map[string]durable.OperationHookInfo, count)
			want := *NewRecord(resumeTestARN)
			want.FunctionName = "order-processor"
			want.Status = StatusRunning
			want.StartTimestamp = &start
			want.Truncated = true
			for i := range count {
				id := fmt.Sprintf("op-%d", count-i)
				op := resumeOp(id, "", "STEP", durable.PluginOperationSucceeded, start.Add(time.Duration(i)*time.Second))
				op.Name = strings.Repeat("n", 128)
				if dropResults {
					op.Result = strings.Repeat("x", 256)
				}
				ops[id] = op
				rec := toOperationRecord(id, op)
				if i < count-1 {
					if !dropResults {
						continue
					}
					rec.Result = nil
					rec.Truncated = true
				}
				want.Operations = append(want.Operations, rec)
			}
			if !dropResults {
				want.DroppedOperations = count - 1
			}
			encoded, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			// EmittedAt changes during dispatch. Allow timestamp-length variation,
			// but less than the space needed to retain another result or operation.
			exp := &resumeLimitedExporter{maxBytes: len(encoded) + 32}
			factory := New(Config{EmitMode: EmitAlways, Exporters: []Exporter{exp}})
			pl := resumeInvocation(t, factory, durable.InvocationHookInfo{
				ExecutionArn:            resumeTestARN,
				RequestID:               "resumed-request",
				ExecutionStartTimestamp: start,
				Operations:              ops,
			})
			// Re-reporting an older operation must update its original slot.
			pl.OnOperationEnd(t.Context(), ops["op-8"])
			pl.OnInvocationEnd(t.Context(), durable.InvocationEndHookInfo{
				ExecutionArn: resumeTestARN,
				Status:       durable.PluginInvocationPending,
			})
			records := exp.snapshot()
			if len(records) != 1 {
				t.Fatalf("exported %d records, want 1", len(records))
			}
			got := records[0]
			if !got.Truncated || got.DroppedOperations != want.DroppedOperations || len(got.Operations) != len(want.Operations) {
				t.Fatalf("truncation = (%v, %d dropped, %d kept), want (true, %d dropped, %d kept)",
					got.Truncated, got.DroppedOperations, len(got.Operations), want.DroppedOperations, len(want.Operations))
			}
			for _, op := range got.Operations {
				if dropResults {
					if op.ID == "op-1" {
						if op.Result == nil || op.Result.Value != ops[op.ID].Result || op.Truncated {
							t.Errorf("newest operation lost its result: %+v", op)
						}
					} else if op.Result != nil || !op.Truncated {
						t.Errorf("older operation retained its result: %+v", op)
					}
				} else if op.ID != "op-1" {
					t.Errorf("retained operation %q, want newest op-1", op.ID)
				}
			}
			encoded, err = json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if len(encoded) > exp.maxBytes {
				t.Errorf("exported %d bytes, exceeds limit %d", len(encoded), exp.maxBytes)
			}
		})
	}
}
