package dag

import (
	"errors"
	"testing"
)

// C6: a task-level option applied to an operation that does not honor it is
// rejected at registration instead of being silently dropped.
func TestRegister_RejectsMisappliedOption(t *testing.T) {
	d := newContext("")
	// WithTimeout applies to Callback/WaitForCondition, not Step.
	Step(d, "s", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithTimeout(Duration{Seconds: 1}))

	var opErr *DagInapplicableOptionError
	found := false
	for _, e := range d.regErrs {
		if errors.As(e, &opErr) && opErr.Task == "s" && opErr.Option == "WithTimeout" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected DagInapplicableOptionError for WithTimeout on Step, got %v", d.regErrs)
	}
}

// C6: the DAG-level WithMaxConcurrency is rejected on a task; the task-level
// WithBatchMaxConcurrency is accepted on Map/Parallel.
func TestRegister_MaxConcurrencyScoping(t *testing.T) {
	d := newContext("")
	Step(d, "s", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }, WithMaxConcurrency(2))
	Map(d, "m", nil, func(_ Deps) []int { return nil },
		func(_ DurableContext, item int, _ int) (int, error) { return item, nil },
		WithBatchMaxConcurrency(3))

	rejected := false
	for _, e := range d.regErrs {
		var opErr *DagInapplicableOptionError
		if errors.As(e, &opErr) && opErr.Task == "s" && opErr.Option == "WithMaxConcurrency" {
			rejected = true
		}
		if errors.As(e, &opErr) && opErr.Task == "m" {
			t.Fatalf("WithBatchMaxConcurrency should be accepted on Map, got %v", e)
		}
	}
	if !rejected {
		t.Fatalf("expected WithMaxConcurrency rejected on Step, got %v", d.regErrs)
	}
}

// C7: WaitForCondition without WithCondition is a registration error.
func TestWaitForCondition_RequiresCondition(t *testing.T) {
	d := newContext("")
	WaitForCondition(d, "poll", nil, 0, func(_ Deps, s int, _ StepContext) (int, error) { return s + 1, nil })

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
	d2 := newContext("")
	WaitForCondition(d2, "poll", nil, 0, func(_ Deps, s int, _ StepContext) (int, error) { return s + 1, nil },
		WithCondition(func(s int) bool { return s >= 3 }))
	if len(d2.regErrs) != 0 {
		t.Fatalf("expected no registration errors with WithCondition, got %v", d2.regErrs)
	}
}

// SubDag intentionally accepts both task-level and DAG-level options
// (documented dual-level forwarding), so no misapplication error is raised.
func TestSubDag_AcceptsAllOptions(t *testing.T) {
	d := newContext("")
	SubDag(d, "sub", nil, func(_ *Context) {}, WithMaxConcurrency(2), WithTriggerRule(AllDone))
	for _, e := range d.regErrs {
		var opErr *DagInapplicableOptionError
		if errors.As(e, &opErr) {
			t.Fatalf("SubDag should accept all options, got %v", e)
		}
	}
}
