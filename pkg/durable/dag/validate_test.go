package dag

import (
	"errors"
	"strings"
	"testing"
)

// buildGraph registers tasks with the given (name -> dep names) edges via a
// plain step per task, returning the Context for validation testing.
func buildGraph(t *testing.T, edges map[string][]string, order []string) *Context {
	t.Helper()
	d := newContext("")
	handles := map[string]TaskHandle[int]{}
	// First pass: register all with no deps (so handles exist).
	for _, name := range order {
		handles[name] = Step(d, name, nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	}
	// Second pass: wire ordering deps.
	for _, name := range order {
		for _, dep := range edges[name] {
			if h, ok := handles[dep]; ok {
				handles[name].DependsOn(h)
			} else {
				// foreign/missing dep: attach by mutating def directly.
				d.byName[name].orderDeps = append(d.byName[name].orderDeps, dep)
			}
		}
	}
	return d
}

func TestValidate_SelfLoop(t *testing.T) {
	d := buildGraph(t, map[string][]string{"a": {"a"}}, []string{"a"})
	err := validate(d, config{})
	var ve *DagValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected DagValidationError, got %v", err)
	}
	var ce *DagCyclicDependencyError
	if !errors.As(err, &ce) {
		t.Fatalf("expected cyclic dependency error, got %v", err)
	}
}

func TestValidate_TwoCycleAndDeepCycle(t *testing.T) {
	d2 := buildGraph(t, map[string][]string{"a": {"b"}, "b": {"a"}}, []string{"a", "b"})
	if err := validate(d2, config{}); err == nil {
		t.Fatal("expected 2-cycle to fail")
	}
	deep := buildGraph(t, map[string][]string{"a": {"c"}, "b": {"a"}, "c": {"b"}}, []string{"a", "b", "c"})
	var ce *DagCyclicDependencyError
	if err := validate(deep, config{}); !errors.As(err, &ce) {
		t.Fatalf("expected deep cycle error, got %v", err)
	}
}

func TestValidate_DiamondNoCycle(t *testing.T) {
	// a -> b, a -> c, b -> d, c -> d  (edges expressed as name depends-on)
	d := buildGraph(t, map[string][]string{
		"b": {"a"}, "c": {"a"}, "d": {"b", "c"},
	}, []string{"a", "b", "c", "d"})
	if err := validate(d, config{}); err != nil {
		t.Fatalf("diamond should be acyclic, got %v", err)
	}
}

func TestValidate_NameViolations(t *testing.T) {
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

func TestValidate_Duplicates(t *testing.T) {
	d := newContext("")
	Step(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	Step(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	var de *DagDuplicateTaskError
	if err := validate(d, config{}); !errors.As(err, &de) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestValidate_MissingDep(t *testing.T) {
	d := newContext("")
	a := Step(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	_ = a
	// b depends on a non-registered task "ghost".
	d.byName["a"].orderDeps = append(d.byName["a"].orderDeps, "ghost")
	var me *DagInvalidDependencyError
	if err := validate(d, config{}); !errors.As(err, &me) {
		t.Fatalf("expected missing dependency error, got %v", err)
	}
}

func TestValidate_ConfigGuards(t *testing.T) {
	d := newContext("")
	Step(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	n := 0
	var ce *DagInvalidConfigError
	if err := validate(d, config{maxConcurrency: &n}); !errors.As(err, &ce) {
		t.Fatalf("expected config error for maxConcurrency<=0, got %v", err)
	}
	min := 1
	tol := 0
	cc := &DagCompletionConfig{
		ShouldComplete:        func(DagCompletionStatus) CompletionDecision { return ContinueDag() },
		MinSuccessful:         &min,
		ToleratedFailureCount: &tol,
	}
	if err := validate(d, config{completion: cc}); !errors.As(err, &ce) {
		t.Fatalf("expected config error for mutually-exclusive completion, got %v", err)
	}
}

func TestValidate_UnknownTriggerRule(t *testing.T) {
	d := newContext("")
	Step(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil }).WithTrigger("BOGUS")
	var te *DagInvalidTriggerRuleError
	if err := validate(d, config{}); !errors.As(err, &te) {
		t.Fatalf("expected invalid trigger rule error, got %v", err)
	}
}

func TestValidate_AggregatedMultiError(t *testing.T) {
	d := newContext("")
	Step(d, "bad-name", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	Step(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	Step(d, "dup", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	err := validate(d, config{})
	var ve *DagValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected aggregate error, got %v", err)
	}
	if len(ve.Errs) < 2 {
		t.Fatalf("expected multiple aggregated errors, got %d: %v", len(ve.Errs), ve.Errs)
	}
}

func TestValidate_UnknownDefaultTriggerRule(t *testing.T) {
	d := newContext("")
	Step(d, "a", nil, func(_ Deps, _ StepContext) (int, error) { return 0, nil })
	var te *DagInvalidTriggerRuleError
	if err := validate(d, config{defaultTrigger: "NONSENSE"}); !errors.As(err, &te) {
		t.Fatalf("expected invalid (default) trigger rule error, got %v", err)
	}
	// A valid default passes.
	if err := validate(d, config{defaultTrigger: AllDone}); err != nil {
		t.Fatalf("valid default trigger should pass, got %v", err)
	}
}
