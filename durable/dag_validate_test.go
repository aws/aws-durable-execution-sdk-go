package durable

import (
	"errors"
	"strings"
	"testing"
)

// dagBuildGraph registers tasks with the given (name -> dep names) edges via
// a plain step per task, returning the DagBuilder for validation testing.
func dagBuildGraph(t *testing.T, edges map[string][]string, order []string) *DagBuilder {
	t.Helper()
	d := newDagBuilder()
	handles := map[string]TaskHandle[int]{}
	// First pass: register all with no deps (so handles exist).
	for _, name := range order {
		handles[name] = DagStep(d, name, nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	// Second pass: wire ordering deps.
	for _, name := range order {
		for _, dep := range edges[name] {
			if h, ok := handles[dep]; ok {
				handles[name].After(h)
			} else {
				// foreign/missing dep: attach by mutating def directly.
				d.byName[name].orderDeps = append(d.byName[name].orderDeps, dep)
			}
		}
	}
	return d
}

func TestDagValidate_SelfLoop(t *testing.T) {
	d := dagBuildGraph(t, map[string][]string{"a": {"a"}}, []string{"a"})
	err := validateDag(d, dagConfig{})
	var ve *DagValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected DagValidationError, got %v", err)
	}
	var ce *DagCyclicDependencyError
	if !errors.As(err, &ce) {
		t.Fatalf("expected cyclic dependency error, got %v", err)
	}
}

func TestDagValidate_TwoCycleAndDeepCycle(t *testing.T) {
	d2 := dagBuildGraph(t, map[string][]string{"a": {"b"}, "b": {"a"}}, []string{"a", "b"})
	if err := validateDag(d2, dagConfig{}); err == nil {
		t.Fatal("expected 2-cycle to fail")
	}
	deep := dagBuildGraph(t, map[string][]string{"a": {"c"}, "b": {"a"}, "c": {"b"}}, []string{"a", "b", "c"})
	var ce *DagCyclicDependencyError
	if err := validateDag(deep, dagConfig{}); !errors.As(err, &ce) {
		t.Fatalf("expected deep cycle error, got %v", err)
	}
}

func TestDagValidate_DiamondNoCycle(t *testing.T) {
	// a -> b, a -> c, b -> d, c -> d  (edges expressed as name depends-on)
	d := dagBuildGraph(t, map[string][]string{
		"b": {"a"}, "c": {"a"}, "d": {"b", "c"},
	}, []string{"a", "b", "c", "d"})
	if err := validateDag(d, dagConfig{}); err != nil {
		t.Fatalf("diamond should be acyclic, got %v", err)
	}
}

func TestDagValidate_NameViolations(t *testing.T) {
	cases := []struct{ name, frag string }{
		{"", "non-empty"},
		{strings.Repeat("x", 101), "100 characters"},
		{"has-dash", "no dashes"},
		{"weird!", "no dashes"},
		{"pre_DAG_NODE_T_x", "reserved"},
	}
	for _, tc := range cases {
		if err := validateName(tc.name); err == nil || !strings.Contains(err.Error(), tc.frag) {
			t.Fatalf("name %q: expected error containing %q, got %v", tc.name, tc.frag, err)
		}
	}
	if err := validateName("valid_Name_123"); err != nil {
		t.Fatalf("valid name rejected: %v", err)
	}
}

func TestDagValidate_Duplicates(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	var de *DagDuplicateTaskError
	if err := validateDag(d, dagConfig{}); !errors.As(err, &de) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestDagValidate_MissingDep(t *testing.T) {
	d := newDagBuilder()
	a := DagStep(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	_ = a
	// depend on a non-registered task "ghost".
	d.byName["a"].orderDeps = append(d.byName["a"].orderDeps, "ghost")
	var me *DagInvalidDependencyError
	if err := validateDag(d, dagConfig{}); !errors.As(err, &me) {
		t.Fatalf("expected missing dependency error, got %v", err)
	}
}

func TestDagValidate_ConfigGuards(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	n := -1
	var ce *DagInvalidConfigError
	if err := validateDag(d, dagConfig{maxConcurrency: &n}); !errors.As(err, &ce) {
		t.Fatalf("expected config error for negative maxConcurrency, got %v", err)
	}
	// 0 is the explicit unbounded sentinel, not an error.
	zero := 0
	if err := validateDag(d, dagConfig{maxConcurrency: &zero}); err != nil {
		t.Fatalf("maxConcurrency=0 (unbounded sentinel) should be valid, got %v", err)
	}
	minv := 1
	tol := 0
	cc := &DagCompletionConfig{
		ShouldComplete:        func(DagCompletionStatus) CompletionDecision { return ContinueDag() },
		MinSuccessful:         &minv,
		ToleratedFailureCount: &tol,
	}
	if err := validateDag(d, dagConfig{completion: cc}); !errors.As(err, &ce) {
		t.Fatalf("expected config error for mutually-exclusive completion, got %v", err)
	}
}

func TestDagValidate_UnknownTriggerRule(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }).WithTrigger("BOGUS")
	var te *DagInvalidTriggerRuleError
	if err := validateDag(d, dagConfig{}); !errors.As(err, &te) {
		t.Fatalf("expected invalid trigger rule error, got %v", err)
	}
}

func TestDagValidate_AggregatedMultiError(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "bad-name", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	DagStep(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	err := validateDag(d, dagConfig{})
	var ve *DagValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected aggregate error, got %v", err)
	}
	if len(ve.Errs) < 2 {
		t.Fatalf("expected multiple aggregated errors, got %d: %v", len(ve.Errs), ve.Errs)
	}
}

func TestDagValidate_UnknownDefaultTriggerRule(t *testing.T) {
	d := newDagBuilder()
	DagStep(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	var te *DagInvalidTriggerRuleError
	if err := validateDag(d, dagConfig{defaultTrigger: "NONSENSE"}); !errors.As(err, &te) {
		t.Fatalf("expected invalid (default) trigger rule error, got %v", err)
	}
	// A valid default passes.
	if err := validateDag(d, dagConfig{defaultTrigger: AllDone}); err != nil {
		t.Fatalf("valid default trigger should pass, got %v", err)
	}
}
