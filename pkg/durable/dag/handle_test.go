package dag

import (
	"errors"
	"testing"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
)

func TestHandle_DependsOnAndWithTriggerMutateDef(t *testing.T) {
	d := newContext("")
	a := Step(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	b := Step(d, "b", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	h := Step(d, "c", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	h.DependsOn(a, b).WithTrigger(AllDone)

	def := d.byName["c"]
	if len(def.orderDeps) != 2 || def.orderDeps[0] != "a" || def.orderDeps[1] != "b" {
		t.Fatalf("DependsOn did not mutate orderDeps: %v", def.orderDeps)
	}
	if !def.hasTrigger || def.trigger != AllDone {
		t.Fatalf("WithTrigger did not mutate trigger: %v", def.trigger)
	}
}

func TestDeps_GetTypedAndErrors(t *testing.T) {
	a := TaskHandle[int]{name: "a", kind: kindPlain}
	deps := newDeps(map[string]any{"a": 7})
	v, err := Get(deps, a)
	if err != nil || v != 7 {
		t.Fatalf("Get: v=%v err=%v", v, err)
	}
	// Missing dep.
	miss := TaskHandle[int]{name: "missing", kind: kindPlain}
	if _, err := Get(deps, miss); !errors.Is(err, ErrDepNotAvailable) {
		t.Fatalf("expected ErrDepNotAvailable, got %v", err)
	}
	// Type mismatch.
	badType := TaskHandle[string]{name: "a", kind: kindPlain}
	if _, err := Get(deps, badType); !errors.Is(err, ErrDepTypeMismatch) {
		t.Fatalf("expected ErrDepTypeMismatch, got %v", err)
	}
}

func TestDeps_MustGetPanicsOnMissing(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustGet should panic on missing dep")
		}
	}()
	MustGet(newDeps(nil), TaskHandle[int]{name: "x"})
}

func TestTrigger_TruthTable(t *testing.T) {
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
		// OneSuccess
		{OneSuccess, nil, false},
		{OneSuccess, []TaskStatus{F, S}, true},
		{OneSuccess, []TaskStatus{F, K}, false},
		// OneFailed
		{OneFailed, nil, false},
		{OneFailed, []TaskStatus{S, F}, true},
		{OneFailed, []TaskStatus{S, K}, false},
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

func TestEntityID_DisjointAndNestedRecursion(t *testing.T) {
	// Entity IDs for prefixed/unprefixed and nested recursion are covered
	// in the context package; here we assert the dag delimiter contract
	// holds through TaskEntityID as used by the wiring.
	root := dcontext.TaskEntityID("", "a")
	if root != "DAG_NODE_T_a" {
		t.Fatalf("unprefixed entity id: %q", root)
	}
	nested := dcontext.TaskEntityID(root, "b")
	if nested != "DAG_NODE_T_a-DAG_NODE_T_b" {
		t.Fatalf("nested entity id: %q", nested)
	}
}
