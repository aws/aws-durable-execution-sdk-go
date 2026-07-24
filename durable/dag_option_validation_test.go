package durable

import (
	"errors"
	"testing"
	"time"
)

// A task-level option applied to an operation that does not honor it is
// rejected at registration instead of being silently dropped.
func TestDagRegister_RejectsMisappliedOption(t *testing.T) {
	d := newDagBuilder()
	// WithTaskTimeout applies to Callback/WaitForCondition, not Step.
	DagStep(d, "s", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithTaskTimeout(time.Second))

	var opErr *DagInapplicableOptionError
	found := false
	for _, e := range d.regErrs {
		if errors.As(e, &opErr) && opErr.Task == "s" && opErr.Option == "WithTaskTimeout" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected DagInapplicableOptionError for WithTaskTimeout on Step, got %v", d.regErrs)
	}
}

// The DAG-level WithDagMaxConcurrency is rejected on a task; the task-level
// WithBatchMaxConcurrency is accepted on Map/Parallel.
func TestDagRegister_MaxConcurrencyScoping(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "s", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithDagMaxConcurrency(2))
	DagMap(d, "m", nil, func(_ Deps) []int { return nil },
		func(_ Context, item int, _ int) (int, error) { return item, nil },
		WithBatchMaxConcurrency(3))

	rejected := false
	for _, e := range d.regErrs {
		var opErr *DagInapplicableOptionError
		if errors.As(e, &opErr) && opErr.Task == "s" && opErr.Option == "WithDagMaxConcurrency" {
			rejected = true
		}
		if errors.As(e, &opErr) && opErr.Task == "m" {
			t.Fatalf("WithBatchMaxConcurrency should be accepted on Map, got %v", e)
		}
	}
	if !rejected {
		t.Fatalf("expected WithDagMaxConcurrency rejected on Step, got %v", d.regErrs)
	}
}

// WaitForCondition without WithCondition is a registration error.
func TestDagWaitForCondition_RequiresCondition(t *testing.T) {
	d := newDagBuilder()
	DagWaitForCondition(d, "poll", nil, 0, func(_ Deps, s int, _ StepContext) (int, error) { return s + 1, nil })

	missing := false
	for _, e := range d.regErrs {
		var cfgErr *DagInvalidConfigError
		if errors.As(e, &cfgErr) {
			missing = true
		}
	}
	if !missing {
		t.Fatalf("expected DagInvalidConfigError when WithCondition is omitted, got %v", d.regErrs)
	}

	// With a condition supplied, no registration error.
	d2 := newDagBuilder()
	DagWaitForCondition(d2, "poll", nil, 0, func(_ Deps, s int, _ StepContext) (int, error) { return s + 1, nil },
		WithCondition(func(s int) bool { return s >= 3 }))
	if len(d2.regErrs) != 0 {
		t.Fatalf("expected no registration errors with WithCondition, got %v", d2.regErrs)
	}
}

// SubDag intentionally accepts both task-level and DAG-level options
// (documented dual-level forwarding), so no misapplication error is raised.
func TestDagSubDag_AcceptsAllOptions(t *testing.T) {
	d := newDagBuilder()
	SubDag(d, "sub", nil, func(_ *DagBuilder) {}, WithDagMaxConcurrency(2), WithTriggerRule(AllDone))
	for _, e := range d.regErrs {
		var opErr *DagInapplicableOptionError
		if errors.As(e, &opErr) {
			t.Fatalf("SubDag should accept all options, got %v", e)
		}
	}
}
