package dag

import (
	"errors"
	"testing"
	"time"
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

	if r.SuccessCount() != 1 || r.FailureCount() != 1 || r.SkippedCount() != 1 || r.TotalCount() != 3 {
		t.Fatalf("counts wrong: s=%d f=%d k=%d t=%d", r.SuccessCount(), r.FailureCount(), r.SkippedCount(), r.TotalCount())
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
	if ok.Err() != nil {
		t.Fatalf("expected nil Err, got %v", ok.Err())
	}
	// Failure -> *DagExecutionError wrapping cause.
	cause := errors.New("root cause")
	bad := newDagResult([]TaskExecution{mkExec("x", StatusFailed, nil, cause)}, CompletedWithFailures)
	err := bad.Err()
	var de *DagExecutionError
	if !errors.As(err, &de) || de.FirstFailed != "x" {
		t.Fatalf("expected DagExecutionError for x, got %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("Err should unwrap to cause")
	}
	// Custom completion failed -> Err non-nil even with no FAILED tasks.
	cust := newDagResult([]TaskExecution{mkExec("a", StatusSucceeded, 1, nil)}, CustomCompletionFailed)
	if cust.Err() == nil {
		t.Fatal("custom-completion-failed should surface Err")
	}
}

func TestDagResult_JSONRoundTrip_WithErrorAndLazyTyping(t *testing.T) {
	r := newDagResult([]TaskExecution{
		mkExec("a", StatusSucceeded, map[string]int{"n": 7}, nil),
		mkExec("b", StatusFailed, nil, errors.New("kaboom")),
		{Name: "c", Status: StatusSkipped, SkipReason: SkipRunIf, kind: kindPlain},
	}, CompletedWithFailures)

	data, err := SerializeDagResult(r)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	back, err := RestoreDagResult(data)
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
	if back.Err() == nil {
		t.Fatal("restored result should surface Err for failed task")
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

	data, err := SerializeDagResult(outer)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	back, err := RestoreDagResult(data)
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
