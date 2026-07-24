package dag

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
)

func mkExec(name string, status TaskStatus, result any, err error) TaskExecution {
	return TaskExecution{Name: name, Status: status, result: result, Err: err, kind: kindPlain, CompletedAt: time.Now()}
}

func TestDagResult_Getters(t *testing.T) {
	r := newDagResult([]TaskExecution{
		mkExec("a", StatusSucceeded, 10, nil),
		mkExec("b", StatusFailed, nil, errors.New("boom")),
		{Name: "c", Status: StatusSkipped, SkipReason: SkipTriggerRule, kind: kindPlain},
	}, CompletedWithFailures)

	if r.SucceededCount() != 1 || r.FailureCount() != 1 || r.SkippedCount() != 1 || r.TotalCount() != 3 {
		t.Fatalf("counts wrong: s=%d f=%d k=%d t=%d", r.SucceededCount(), r.FailureCount(), r.SkippedCount(), r.TotalCount())
	}
	if st, ok := r.Status("a"); !ok || st != StatusSucceeded {
		t.Fatalf("status a: %v %v", st, ok)
	}
	if _, ok := r.Status("missing"); ok {
		t.Fatal("missing task should return not-started (false)")
	}
	if len(r.Succeeded()) != 1 || len(r.Failed()) != 1 || len(r.Skipped()) != 1 {
		t.Fatal("filter getters wrong")
	}
}

func TestDagResult_ResultTyped(t *testing.T) {
	h := TaskHandle[int]{name: "a", kind: kindPlain}
	r := newDagResult([]TaskExecution{mkExec("a", StatusSucceeded, 42, nil)}, AllCompleted)
	v, err := Result(r, h)
	if err != nil || v != 42 {
		t.Fatalf("Result: v=%v err=%v", v, err)
	}
	// failed/absent -> ErrDepNotAvailable
	hf := TaskHandle[int]{name: "nope", kind: kindPlain}
	if _, err := Result(r, hf); !errors.Is(err, ErrDepNotAvailable) {
		t.Fatalf("expected ErrDepNotAvailable, got %v", err)
	}
}

func TestDagResult_Err(t *testing.T) {
	// No failures -> nil.
	ok := newDagResult([]TaskExecution{mkExec("a", StatusSucceeded, 1, nil)}, AllCompleted)
	if ok.ThrowIfError() != nil {
		t.Fatalf("expected nil ThrowIfError, got %v", ok.ThrowIfError())
	}
	// Failure -> *DagExecutionError wrapping cause.
	cause := errors.New("root cause")
	bad := newDagResult([]TaskExecution{mkExec("x", StatusFailed, nil, cause)}, CompletedWithFailures)
	err := bad.ThrowIfError()
	var de *DagExecutionError
	if !errors.As(err, &de) || de.FirstFailed != "x" {
		t.Fatalf("expected DagExecutionError for x, got %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("ThrowIfError should unwrap to cause")
	}
	// Custom completion failed -> Err non-nil even with no FAILED tasks.
	cust := newDagResult([]TaskExecution{mkExec("a", StatusSucceeded, 1, nil)}, CustomCompletionFailed)
	if cust.ThrowIfError() == nil {
		t.Fatal("custom-completion-failed should surface ThrowIfError")
	}
}

func TestDagResult_JSONRoundTrip_WithErrorAndLazyTyping(t *testing.T) {
	r := newDagResult([]TaskExecution{
		mkExec("a", StatusSucceeded, map[string]int{"n": 7}, nil),
		mkExec("b", StatusFailed, nil, errors.New("kaboom")),
		{Name: "c", Status: StatusSkipped, SkipReason: SkipRunIf, kind: kindPlain},
	}, CompletedWithFailures)

	data, err := serializeDagResult(r)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	back, err := restoreDagResult(data)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if back.CompletionReason() != CompletedWithFailures {
		t.Fatalf("reason lost: %v", back.CompletionReason())
	}
	// Lazy typing of a succeeded result.
	ha := TaskHandle[map[string]int]{name: "a", kind: kindPlain}
	v, err := Result(back, ha)
	if err != nil || v["n"] != 7 {
		t.Fatalf("lazy typed result: v=%v err=%v", v, err)
	}
	// Error reconstruction.
	if st, _ := back.Status("b"); st != StatusFailed {
		t.Fatalf("b status: %v", st)
	}
	if back.ThrowIfError() == nil {
		t.Fatal("restored result should surface ThrowIfError for failed task")
	}
	// Skip reason preserved.
	if len(back.Skipped()) != 1 || back.Skipped()[0].SkipReason != SkipRunIf {
		t.Fatalf("skip reason lost: %v", back.Skipped())
	}
}

func TestDagResult_NestedDagRecursion(t *testing.T) {
	inner := newDagResult([]TaskExecution{mkExec("leaf", StatusSucceeded, 99, nil)}, AllCompleted)
	outer := newDagResult([]TaskExecution{
		{Name: "sub", Status: StatusSucceeded, result: inner, kind: kindDag, CompletedAt: time.Now()},
	}, AllCompleted)

	data, err := serializeDagResult(outer)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	back, err := restoreDagResult(data)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	hsub := TaskHandle[*DagResult]{name: "sub", kind: kindDag}
	sub, err := Result(back, hsub)
	if err != nil {
		t.Fatalf("nested Result: %v", err)
	}
	hleaf := TaskHandle[int]{name: "leaf", kind: kindPlain}
	leaf, err := Result(sub, hleaf)
	if err != nil || leaf != 99 {
		t.Fatalf("nested leaf value: v=%v err=%v", leaf, err)
	}
}

// TestDagResult_BatchKindRoundTrip guards fix #4: a kindBatch task result
// (a Map/Parallel BatchResult stored as json.RawMessage) survives the
// serialize/restore cycle and is lazily typed via Result[BatchResult[T]].
// Before BatchResult.UnmarshalJSON existed, the restore path failed because
// json.Unmarshal could not decode ItemResult.Err (an error interface).
func TestDagResult_BatchKindRoundTrip(t *testing.T) {
	br := BatchResult[int]{
		Items: []operations.ItemResult[int]{
			{Value: 10},
			{Value: 20, Err: errors.New("boom")},
		},
		CompletionReason: operations.CompletionReasonAllCompleted,
	}
	r := newDagResult([]TaskExecution{
		{Name: "batchtask", Status: StatusSucceeded, result: br, kind: kindBatch, CompletedAt: time.Now()},
	}, AllCompleted)

	data, err := serializeDagResult(r)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	back, err := restoreDagResult(data)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	h := TaskHandle[BatchResult[int]]{name: "batchtask", kind: kindBatch}
	got, err := Result(back, h)
	if err != nil {
		t.Fatalf("Result[BatchResult[int]]: %v", err)
	}
	if len(got.Items) != 2 || got.Items[0].Value != 10 || got.Items[1].Value != 20 {
		t.Fatalf("batch item values lost: %+v", got.Items)
	}
	if got.Items[1].Err == nil {
		t.Fatalf("batch item error string lost on round-trip")
	}
	if got.CompletionReason != operations.CompletionReasonAllCompleted {
		t.Fatalf("batch completion reason lost: %v", got.CompletionReason)
	}
}
