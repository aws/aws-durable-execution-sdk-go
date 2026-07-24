package durable

import (
	"errors"
	"testing"
	"time"
)

func dagMkExec(name string, status TaskStatus, result any, err error) TaskExecution {
	return TaskExecution{Name: name, Status: status, result: result, Err: err, kind: dagKindPlain, CompletedAt: time.Now()}
}

func TestDagResult_Getters(t *testing.T) {
	r := newDagResult([]TaskExecution{
		dagMkExec("a", StatusSucceeded, 10, nil),
		dagMkExec("b", StatusFailed, nil, errors.New("boom")),
		{Name: "c", Status: StatusSkipped, SkipReason: SkipTriggerRule, kind: dagKindPlain},
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
	h := TaskHandle[int]{name: "a", kind: dagKindPlain}
	r := newDagResult([]TaskExecution{dagMkExec("a", StatusSucceeded, 42, nil)}, AllCompleted)
	v, err := Result(r, h)
	if err != nil || v != 42 {
		t.Fatalf("Result: v=%v err=%v", v, err)
	}
	// failed/absent -> ErrDepNotAvailable
	hf := TaskHandle[int]{name: "nope", kind: dagKindPlain}
	if _, err := Result(r, hf); !errors.Is(err, ErrDepNotAvailable) {
		t.Fatalf("expected ErrDepNotAvailable, got %v", err)
	}
}

func TestDagResult_ThrowIfError(t *testing.T) {
	// No failures -> nil.
	ok := newDagResult([]TaskExecution{dagMkExec("a", StatusSucceeded, 1, nil)}, AllCompleted)
	if ok.ThrowIfError() != nil {
		t.Fatalf("expected nil ThrowIfError, got %v", ok.ThrowIfError())
	}
	// Failure -> *DagExecutionError wrapping cause.
	cause := errors.New("root cause")
	bad := newDagResult([]TaskExecution{dagMkExec("x", StatusFailed, nil, cause)}, CompletedWithFailures)
	err := bad.ThrowIfError()
	var de *DagExecutionError
	if !errors.As(err, &de) || de.FirstFailed != "x" {
		t.Fatalf("expected DagExecutionError for x, got %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("ThrowIfError should unwrap to cause")
	}
	// Custom completion failed -> non-nil even with no FAILED tasks.
	cust := newDagResult([]TaskExecution{dagMkExec("a", StatusSucceeded, 1, nil)}, CustomCompletionFailed)
	if cust.ThrowIfError() == nil {
		t.Fatal("custom-completion-failed should surface ThrowIfError")
	}
}

// TestDagResult_ResultByNameLazyTyping proves ResultByName decodes a
// JSON-encoded result on the replay path (rawResult) into the requested
// type — the alpha reconstruction path that replaces the source's
// serialize/restore round-trip (alpha does not persist an aggregate
// DagResult checkpoint; see DAG_INVENTORY §2.7).
func TestDagResult_ResultByNameLazyTyping(t *testing.T) {
	raw := []byte(`{"n":7}`)
	r := newDagResult([]TaskExecution{
		{Name: "a", Status: StatusSucceeded, rawResult: raw, kind: dagKindPlain},
	}, AllCompleted)
	v, err := ResultByName[map[string]int](r, "a")
	if err != nil || v["n"] != 7 {
		t.Fatalf("lazy typed result: v=%v err=%v", v, err)
	}
}

// TestDagTaskFailedError_Taxonomy covers the error-taxonomy contract:
// DagTaskFailedError unwraps to its cause and matches *OperationError via
// errors.As (mirroring BatchItemFailedError); DagError does likewise at the
// scope level.
func TestDagTaskFailedError_Taxonomy(t *testing.T) {
	cause := errors.New("inner boom")
	te := &DagTaskFailedError{Name: "t1", TaskID: "id1", Err: cause}
	if !errors.Is(te, cause) {
		t.Fatalf("DagTaskFailedError should unwrap to cause")
	}
	var oe *OperationError
	if !errors.As(te, &oe) || oe.Name != "t1" {
		t.Fatalf("DagTaskFailedError should match *OperationError with Name=t1, got %v", oe)
	}

	de := &DagError{Name: "wf", Err: cause}
	if !errors.Is(de, cause) {
		t.Fatalf("DagError should unwrap to cause")
	}
	var oe2 *OperationError
	if !errors.As(de, &oe2) || oe2.Name != "wf" {
		t.Fatalf("DagError should match *OperationError with Name=wf, got %v", oe2)
	}
}
