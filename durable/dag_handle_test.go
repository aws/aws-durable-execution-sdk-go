package durable

import (
	"errors"
	"testing"
)

func TestDagHandle_AfterAndWithTriggerMutateDef(t *testing.T) {
	d := newDagBuilder()
	a := DagStep(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	b := DagStep(d, "b", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	h := DagStep(d, "c", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	h.After(a, b).WithTrigger(AllDone)

	def := d.byName["c"]
	if len(def.orderDeps) != 2 || def.orderDeps[0] != "a" || def.orderDeps[1] != "b" {
		t.Fatalf("After did not mutate orderDeps: %v", def.orderDeps)
	}
	if !def.hasTrigger || def.trigger != AllDone {
		t.Fatalf("WithTrigger did not mutate trigger: %v", def.trigger)
	}
}

func TestDagDeps_GetTypedAndErrors(t *testing.T) {
	a := TaskHandle[int]{name: "a", kind: dagKindPlain}
	deps := newDeps(map[string]any{"a": 7})
	v, err := Get(deps, a)
	if err != nil || v != 7 {
		t.Fatalf("Get: v=%v err=%v", v, err)
	}
	// Missing dep.
	miss := TaskHandle[int]{name: "missing", kind: dagKindPlain}
	if _, err := Get(deps, miss); !errors.Is(err, ErrDepNotAvailable) {
		t.Fatalf("expected ErrDepNotAvailable, got %v", err)
	}
	// Type mismatch.
	badType := TaskHandle[string]{name: "a", kind: dagKindPlain}
	if _, err := Get(deps, badType); !errors.Is(err, ErrDepTypeMismatch) {
		t.Fatalf("expected ErrDepTypeMismatch, got %v", err)
	}
}

func TestDagDeps_MustGetPanicsOnMissing(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustGet should panic on missing dep")
		}
	}()
	MustGet(newDeps(nil), TaskHandle[int]{name: "x"})
}

func TestDagTrigger_TruthTable(t *testing.T) {
	S, F, K := StatusSucceeded, StatusFailed, StatusSkipped
	type row struct {
		rule     TriggerRule
		upstream []TaskStatus
		want     bool
	}
	rows := []row{
		// AllSuccess
		{AllSuccess, nil, true},
		{AllSuccess, []TaskStatus{S, S}, true},
		{AllSuccess, []TaskStatus{S, F}, false},
		{AllSuccess, []TaskStatus{S, K}, false},
		// AllFailed (empty => false due to len>0 guard)
		{AllFailed, nil, false},
		{AllFailed, []TaskStatus{F, F}, true},
		{AllFailed, []TaskStatus{F, S}, false},
		// AllDone (always true)
		{AllDone, nil, true},
		{AllDone, []TaskStatus{S, F, K}, true},
		// AnySuccess
		{AnySuccess, nil, false},
		{AnySuccess, []TaskStatus{F, S}, true},
		{AnySuccess, []TaskStatus{F, K}, false},
		// AnyFailed
		{AnyFailed, nil, false},
		{AnyFailed, []TaskStatus{S, F}, true},
		{AnyFailed, []TaskStatus{S, K}, false},
		// NoneFailed (empty => true; skips allowed)
		{NoneFailed, nil, true},
		{NoneFailed, []TaskStatus{S, K}, true},
		{NoneFailed, []TaskStatus{S, F}, false},
	}
	for i, r := range rows {
		if got := evaluateTrigger(r.rule, r.upstream); got != r.want {
			t.Fatalf("row %d rule=%s upstream=%v: got %v want %v", i, r.rule, r.upstream, got, r.want)
		}
	}
	// Empty rule defaults to AllSuccess.
	if !evaluateTrigger("", nil) {
		t.Fatal("empty rule should default to AllSuccess (true for no upstream)")
	}
	// Unknown rule -> false (defensive; validation rejects it).
	if evaluateTrigger("BOGUS", nil) {
		t.Fatal("unknown rule should evaluate false")
	}
}
