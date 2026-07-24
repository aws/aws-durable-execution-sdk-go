// Cross-language DAG conformance suite (Go side).
//
// This test implements every catalog scenario applicable to Go from
// docs/DAG_CONFORMANCE.md (DAG-1..DAG-19, including the [TS+Go] custom
// result-based completion scenario DAG-18), runs each via the SkipTime
// local runner exactly as the existing dag tests do, asserts the actual
// semantic outcome equals the catalog's expected outcome, and emits one
// key-sorted normalized JSON record per scenario to
// /Users/parpooya/workplace/dag-conformance-out/go.json (schema: catalog
// Part B).
//
// Records carry SEMANTIC outcomes only (statuses, results, completion
// reasons, counts, skip reasons, normalized error types) plus per-language
// STRUCTURAL entity-ID checks (name-based, DAG_NODE_T_ delimiter present,
// dash-free names, disjoint from counter IDs). Raw entity-ID hashes are NOT
// compared cross-language.
package dag_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

const conformanceOutPath = "/Users/parpooya/workplace/dag-conformance-out/go.json"

var confNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// ── record builders (catalog Part B schema) ───────────────────────────────

func tSucc(result any) map[string]any {
	return map[string]any{"status": "SUCCEEDED", "result": result, "error_type": nil, "skip_reason": nil}
}

func tFail() map[string]any {
	return map[string]any{"status": "FAILED", "result": nil, "error_type": "StepError", "skip_reason": nil}
}

func tSkip(reason dag.SkipReason) map[string]any {
	return map[string]any{"status": "SKIPPED", "result": nil, "error_type": nil, "skip_reason": string(reason)}
}

func counts(success, failure, skipped, total int) map[string]any {
	return map[string]any{"success": success, "failure": failure, "skipped": skipped, "total": total}
}

// structChecks computes the four per-language structural entity-ID checks
// from the DAG's OWN name-based ID seam (dcontext.TaskEntityID /
// DagNodeDelimiter), exactly as dag_run.go mints task IDs at run time.
// minted=false (validation-error scenarios: no IDs minted) => all false.
// minted=true with no names (empty DAG) => all true (vacuously satisfied).
func structChecks(minted bool, names ...string) map[string]any {
	if !minted {
		return map[string]any{
			"name_based": false, "has_delimiter": false,
			"dash_free": false, "disjoint_from_counter": false,
		}
	}
	nameBased, hasDelim, dashFree, disjoint := true, true, true, true
	for _, n := range names {
		pre := dcontext.TaskEntityID("", n) // hash pre-image the SDK actually hashes
		if strings.Count(pre, dcontext.DagNodeDelimiter) != 1 {
			hasDelim = false
		}
		if !strings.Contains(pre, n) {
			nameBased = false
		}
		if !confNameRe.MatchString(n) {
			dashFree = false
		}
		// A task ID pre-image always contains the delimiter; a sibling
		// counter ID never does => the two ID spaces are disjoint.
		if !strings.Contains(pre, dcontext.DagNodeDelimiter) {
			disjoint = false
		}
	}
	return map[string]any{
		"name_based": nameBased, "has_delimiter": hasDelim,
		"dash_free": dashFree, "disjoint_from_counter": disjoint,
	}
}

func record(scenario string, tasks map[string]any, reason any, c, sc map[string]any, valErr any) map[string]any {
	return map[string]any{
		"scenario":             scenario,
		"tasks":                tasks,
		"completion_reason":    reason,
		"counts":               c,
		"structural_id_checks": sc,
		"validation_error":     valErr,
	}
}

// liveRecord assembles a record from a live DagResult (non-validation
// scenarios). tasks is built by the caller (it knows each task's result
// type); names is the registered task-name list for structural checks.
func liveRecord(scenario string, res *dag.DagResult, tasks map[string]any, names ...string) map[string]any {
	return record(
		scenario, tasks, res.CompletionReason(),
		counts(res.SuccessCount(), res.FailureCount(), res.SkippedCount(), res.TotalCount()),
		structChecks(true, names...), nil,
	)
}

func skipReasonOf(res *dag.DagResult, name string) dag.SkipReason {
	return res.Results()[name].SkipReason
}

// classifyValidationError maps a native DAG validation error to the
// catalog's normalized Dag*Error token.
func classifyValidationError(err error) string {
	var ce *dag.DagCyclicDependencyError
	var de *dag.DagDuplicateTaskError
	var ne *dag.DagInvalidTaskNameError
	var pe *dag.DagInvalidDependencyError
	switch {
	case errors.As(err, &ce):
		return "DagCyclicDependencyError"
	case errors.As(err, &de):
		return "DagDuplicateTaskError"
	case errors.As(err, &ne):
		return "DagInvalidTaskNameError"
	case errors.As(err, &pe):
		return "DagInvalidDependencyError"
	}
	return "UNKNOWN"
}

func validationRecord(scenario, token string) map[string]any {
	return record(scenario, map[string]any{}, nil, counts(0, 0, 0, 0), structChecks(false), token)
}

// ── scenario handlers ──────────────────────────────────────────────────────

// DAG-1: diamond fan-out/in with typed deps.
func hDAG1(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "diamond", func(d *dag.Context) {
		fetch := dag.Step(d, "fetch", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 10, nil })
		ta := dag.Step(d, "ta", []dag.AnyHandle{fetch}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			v, _ := dag.Get(dp, fetch)
			return v + 1, nil
		})
		tb := dag.Step(d, "tb", []dag.AnyHandle{fetch}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			v, _ := dag.Get(dp, fetch)
			return v * 2, nil
		})
		dag.Step(d, "merge", []dag.AnyHandle{ta, tb}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			a, _ := dag.Get(dp, ta)
			b, _ := dag.Get(dp, tb)
			return a + b, nil
		})
	})
	if err != nil {
		return nil, err
	}
	fetch, _ := dag.ResultByName[int](res, "fetch")
	ta, _ := dag.ResultByName[int](res, "ta")
	tb, _ := dag.ResultByName[int](res, "tb")
	merge, _ := dag.ResultByName[int](res, "merge")
	tasks := map[string]any{
		"fetch": tSucc(fetch), "ta": tSucc(ta), "tb": tSucc(tb), "merge": tSucc(merge),
	}
	return liveRecord("DAG-1", res, tasks, "fetch", "ta", "tb", "merge"), nil
}

// compensationGraph builds the DAG-2/DAG-3 shared graph; chargeFails toggles
// the guarded task's outcome.
func compensationGraph(scenario string, chargeFails bool) func(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	return func(_ struct{}, dc types.DurableContext) (map[string]any, error) {
		res, err := dag.Dag(dc, "order", func(d *dag.Context) {
			charge := dag.Step(d, "charge", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				if chargeFails {
					return "", errors.New("charge failed")
				}
				return "charged", nil
			})
			dag.Step(d, "fulfill", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				return "fulfilled", nil
			}).DependsOn(charge) // default ALL_SUCCESS
			dag.Step(d, "refund", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				return "refunded", nil
			}).DependsOn(charge).WithTrigger(dag.AllFailed)
			dag.Step(d, "audit", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				return "audited", nil
			}).DependsOn(charge).WithTrigger(dag.AllDone)
		})
		if err != nil {
			return nil, err
		}
		tasks := map[string]any{}
		build := func(name string) {
			st, _ := res.Status(name)
			switch st {
			case dag.StatusSucceeded:
				v, _ := dag.ResultByName[string](res, name)
				tasks[name] = tSucc(v)
			case dag.StatusFailed:
				tasks[name] = tFail()
			case dag.StatusSkipped:
				tasks[name] = tSkip(skipReasonOf(res, name))
			}
		}
		for _, n := range []string{"charge", "fulfill", "refund", "audit"} {
			build(n)
		}
		return liveRecord(scenario, res, tasks, "charge", "fulfill", "refund", "audit"), nil
	}
}

// DAG-4: runIf value-branching (exactly one branch runs).
func hDAG4(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "branch", func(d *dag.Context) {
		classify := dag.Step(d, "classify", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "review", nil
		})
		mk := func(name, want, out string) {
			dag.Step(d, name, []dag.AnyHandle{classify}, func(dp dag.Deps, _ dag.StepContext) (string, error) {
				return out, nil
			}, dag.WithRunIf(func(dp dag.Deps) bool {
				v, _ := dag.Get(dp, classify)
				return v == want
			}))
		}
		mk("publish", "publish", "published")
		mk("review", "review", "reviewed")
		mk("block", "block", "blocked")
	})
	if err != nil {
		return nil, err
	}
	classify, _ := dag.ResultByName[string](res, "classify")
	review, _ := dag.ResultByName[string](res, "review")
	tasks := map[string]any{
		"classify": tSucc(classify),
		"publish":  tSkip(skipReasonOf(res, "publish")),
		"review":   tSucc(review),
		"block":    tSkip(skipReasonOf(res, "block")),
	}
	return liveRecord("DAG-4", res, tasks, "classify", "publish", "review", "block"), nil
}

// triggerMatrixResult builds the record for DAG-5/6/7 style graphs where a
// fixed set of consumer names return a constant string on success.
func triggerMatrixTasks(res *dag.DagResult, succeed map[string]string) map[string]any {
	tasks := map[string]any{}
	for _, te := range res.Results() {
		name := te.Name
		switch te.Status {
		case dag.StatusSucceeded:
			v, _ := dag.ResultByName[string](res, name)
			tasks[name] = tSucc(v)
		case dag.StatusFailed:
			tasks[name] = tFail()
		case dag.StatusSkipped:
			tasks[name] = tSkip(te.SkipReason)
		}
	}
	return tasks
}

// DAG-5: trigger-rule matrix, empty-upstream row (one root per rule).
func hDAG5(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	rules := []struct {
		name string
		rule dag.TriggerRule
	}{
		{"r_all_success", dag.AllSuccess},
		{"r_all_failed", dag.AllFailed},
		{"r_all_done", dag.AllDone},
		{"r_one_success", dag.OneSuccess},
		{"r_one_failed", dag.OneFailed},
		{"r_none_failed", dag.NoneFailed},
	}
	res, err := dag.Dag(dc, "matrix_empty", func(d *dag.Context) {
		for _, r := range rules {
			dag.Step(d, r.name, nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				return "ok", nil
			}, dag.WithTriggerRule(r.rule))
		}
	})
	if err != nil {
		return nil, err
	}
	tasks := triggerMatrixTasks(res, nil)
	names := make([]string, len(rules))
	for i, r := range rules {
		names[i] = r.name
	}
	return liveRecord("DAG-5", res, tasks, names...), nil
}

// DAG-6: trigger-rule matrix, mixed succ/fail upstream.
func hDAG6(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "matrix_mixed", func(d *dag.Context) {
		upOK := dag.Step(d, "up_ok", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) { return "ok", nil })
		upFail := dag.Step(d, "up_fail", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "", errors.New("boom")
		})
		consumers := []struct {
			name string
			rule dag.TriggerRule
		}{
			{"c_all_success", dag.AllSuccess},
			{"c_all_failed", dag.AllFailed},
			{"c_all_done", dag.AllDone},
			{"c_one_success", dag.OneSuccess},
			{"c_one_failed", dag.OneFailed},
			{"c_none_failed", dag.NoneFailed},
		}
		for _, c := range consumers {
			dag.Step(d, c.name, nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				return "c", nil
			}).DependsOn(upOK, upFail).WithTrigger(c.rule)
		}
	})
	if err != nil {
		return nil, err
	}
	tasks := triggerMatrixTasks(res, nil)
	names := []string{"up_ok", "up_fail", "c_all_success", "c_all_failed", "c_all_done", "c_one_success", "c_one_failed", "c_none_failed"}
	return liveRecord("DAG-6", res, tasks, names...), nil
}

// DAG-7: trigger-rule matrix, all-failed upstream.
func hDAG7(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "matrix_allfailed", func(d *dag.Context) {
		u1 := dag.Step(d, "u1", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) { return "", errors.New("boom") })
		u2 := dag.Step(d, "u2", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) { return "", errors.New("boom") })
		consumers := []struct {
			name string
			rule dag.TriggerRule
		}{
			{"k_all_success", dag.AllSuccess},
			{"k_all_failed", dag.AllFailed},
			{"k_all_done", dag.AllDone},
			{"k_one_success", dag.OneSuccess},
			{"k_one_failed", dag.OneFailed},
			{"k_none_failed", dag.NoneFailed},
		}
		for _, c := range consumers {
			dag.Step(d, c.name, nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				return "k", nil
			}).DependsOn(u1, u2).WithTrigger(c.rule)
		}
	})
	if err != nil {
		return nil, err
	}
	tasks := triggerMatrixTasks(res, nil)
	names := []string{"u1", "u2", "k_all_success", "k_all_failed", "k_all_done", "k_one_success", "k_one_failed", "k_none_failed"}
	return liveRecord("DAG-7", res, tasks, names...), nil
}

// DAG-8: skip cascade + "includes SKIPPED" trigger row.
func hDAG8(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "cascade", func(d *dag.Context) {
		seed := dag.Step(d, "seed", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 1, nil })
		gate := dag.Step(d, "gate", []dag.AnyHandle{seed}, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "gate", nil
		}, dag.WithRunIf(func(dp dag.Deps) bool {
			v, _ := dag.Get(dp, seed)
			return v > 100
		}))
		d1 := dag.Step(d, "d1", []dag.AnyHandle{gate}, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "d1", nil
		})
		dag.Step(d, "d2", []dag.AnyHandle{d1}, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "d2", nil
		})
		dag.Step(d, "sink", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "sink", nil
		}).DependsOn(gate).WithTrigger(dag.AllDone)
	})
	if err != nil {
		return nil, err
	}
	seed, _ := dag.ResultByName[int](res, "seed")
	sink, _ := dag.ResultByName[string](res, "sink")
	tasks := map[string]any{
		"seed": tSucc(seed),
		"gate": tSkip(skipReasonOf(res, "gate")),
		"d1":   tSkip(skipReasonOf(res, "d1")),
		"d2":   tSkip(skipReasonOf(res, "d2")),
		"sink": tSucc(sink),
	}
	return liveRecord("DAG-8", res, tasks, "seed", "gate", "d1", "d2", "sink"), nil
}

// DAG-9: nested DAG (result consumed downstream + scope isolation).
func hDAG9(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "outer", func(d *dag.Context) {
		a := dag.Step(d, "a", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 2, nil })
		inner := dag.SubDag(d, "inner", []dag.AnyHandle{a}, func(sub *dag.Context) {
			x := dag.Step(sub, "x", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 3, nil })
			dag.Step(sub, "y", []dag.AnyHandle{x}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
				v, _ := dag.Get(dp, x)
				return v * 10, nil
			})
		})
		dag.Step(d, "consume", []dag.AnyHandle{inner}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			ir, _ := dag.Get(dp, inner)
			y, _ := dag.ResultByName[int](ir, "y")
			return y + 5, nil
		})
	})
	if err != nil {
		return nil, err
	}
	// Scope-isolation assertions: inner task names invisible in outer scope.
	if _, ok := res.Status("x"); ok {
		return nil, errors.New("scope isolation violated: 'x' visible in outer scope")
	}
	if _, ok := res.Status("y"); ok {
		return nil, errors.New("scope isolation violated: 'y' visible in outer scope")
	}
	a, _ := dag.ResultByName[int](res, "a")
	consume, _ := dag.ResultByName[int](res, "consume")
	innerRes, _ := dag.ResultByName[*dag.DagResult](res, "inner")
	innerNorm := map[string]any{
		"completion_reason": innerRes.CompletionReason(),
		"counts":            counts(innerRes.SuccessCount(), innerRes.FailureCount(), innerRes.SkippedCount(), innerRes.TotalCount()),
	}
	tasks := map[string]any{
		"a":       tSucc(a),
		"inner":   tSucc(innerNorm),
		"consume": tSucc(consume),
	}
	return liveRecord("DAG-9", res, tasks, "a", "inner", "consume"), nil
}

// DAG-10: empty DAG.
func hDAG10(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "empty", func(d *dag.Context) {})
	if err != nil {
		return nil, err
	}
	return liveRecord("DAG-10", res, map[string]any{}), nil
}

// DAG-11..15: validation errors.
func hDAG11(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	_, err := dag.Dag(dc, "cycle", func(d *dag.Context) {
		p := dag.Step(d, "p", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 0, nil })
		q := dag.Step(d, "q", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 0, nil })
		p.DependsOn(q)
		q.DependsOn(p)
	})
	if err == nil {
		return nil, errors.New("DAG-11: expected validation error, got nil")
	}
	return validationRecord("DAG-11", classifyValidationError(err)), nil
}

func hDAG12(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	_, err := dag.Dag(dc, "dup", func(d *dag.Context) {
		dag.Step(d, "dup", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 0, nil })
		dag.Step(d, "dup", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 1, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-12: expected validation error, got nil")
	}
	return validationRecord("DAG-12", classifyValidationError(err)), nil
}

func hDAG13(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	_, err := dag.Dag(dc, "dashname", func(d *dag.Context) {
		dag.Step(d, "fetch-data", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 0, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-13: expected validation error, got nil")
	}
	return validationRecord("DAG-13", classifyValidationError(err)), nil
}

func hDAG14(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	_, err := dag.Dag(dc, "reserved", func(d *dag.Context) {
		dag.Step(d, "DAG_NODE_T_root", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 0, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-14: expected validation error, got nil")
	}
	return validationRecord("DAG-14", classifyValidationError(err)), nil
}

func hDAG15(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	// Mint a handle in a DIFFERENT DAG scope, then reference it from scope A.
	var foreign dag.AnyHandle
	if _, e := dag.Dag(dc, "sibling", func(sd *dag.Context) {
		foreign = dag.Step(sd, "foreignNode", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 1, nil })
	}); e != nil {
		return nil, e
	}
	_, err := dag.Dag(dc, "scopeA", func(d *dag.Context) {
		dag.Step(d, "t", []dag.AnyHandle{foreign}, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 2, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-15: expected validation error, got nil")
	}
	return validationRecord("DAG-15", classifyValidationError(err)), nil
}

// DAG-16: minSuccessful threshold early-completion.
func hDAG16(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	min := 3
	res, err := dag.Dag(dc, "minsucc", func(d *dag.Context) {
		var prev dag.AnyHandle
		for i := 1; i <= 5; i++ {
			i := i
			var deps []dag.AnyHandle
			if prev != nil {
				deps = []dag.AnyHandle{prev}
			}
			prev = dag.Step(d, sName(i), deps, func(_ dag.Deps, _ dag.StepContext) (int, error) { return i, nil })
		}
	}, dag.WithMaxConcurrency(1), dag.WithCompletion(dag.DagCompletionConfig{MinSuccessful: &min}))
	if err != nil {
		return nil, err
	}
	tasks := map[string]any{}
	for i := 1; i <= 5; i++ {
		if st, ok := res.Status(sName(i)); ok && st == dag.StatusSucceeded {
			v, _ := dag.ResultByName[int](res, sName(i))
			tasks[sName(i)] = tSucc(v)
		}
	}
	return liveRecord("DAG-16", res, tasks, "s1", "s2", "s3", "s4", "s5"), nil
}

// DAG-17: toleratedFailureCount exceeded early-completion.
func hDAG17(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	tol := 1
	res, err := dag.Dag(dc, "toltol", func(d *dag.Context) {
		var prev dag.AnyHandle
		for i := 1; i <= 4; i++ {
			var deps []dag.AnyHandle
			h := dag.Step(d, tName(i), deps, func(_ dag.Deps, _ dag.StepContext) (int, error) {
				return 0, errors.New("boom")
			})
			if prev != nil {
				h.DependsOn(prev).WithTrigger(dag.AllDone)
			}
			prev = h
		}
	}, dag.WithMaxConcurrency(1), dag.WithCompletion(dag.DagCompletionConfig{ToleratedFailureCount: &tol}))
	if err != nil {
		return nil, err
	}
	tasks := map[string]any{}
	for i := 1; i <= 4; i++ {
		if st, ok := res.Status(tName(i)); ok && st == dag.StatusFailed {
			tasks[tName(i)] = tFail()
		}
	}
	return liveRecord("DAG-17", res, tasks, "t1", "t2", "t3", "t4"), nil
}

// verdict is DAG-18's task result shape.
type verdict struct {
	Verdict string `json:"verdict"`
}

// DAG-18: custom result-based completion [TS + Go ONLY].
func hDAG18(_ struct{}, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "rules", func(d *dag.Context) {
		verdicts := []string{"ACCEPT", "REJECT", "ACCEPT"}
		var prev dag.AnyHandle
		for i, v := range verdicts {
			v := v
			var deps []dag.AnyHandle
			h := dag.Step(d, rName(i+1), deps, func(_ dag.Deps, _ dag.StepContext) (verdict, error) {
				return verdict{Verdict: v}, nil
			})
			if prev != nil {
				h.DependsOn(prev)
			}
			prev = h
		}
	}, dag.WithMaxConcurrency(1), dag.WithCompletion(dag.DagCompletionConfig{
		ShouldComplete: func(st dag.DagCompletionStatus) dag.CompletionDecision {
			for _, it := range st.Items {
				if it.Status == dag.StatusSucceeded {
					if v, ok := dag.ResultOf[verdict](it); ok && v.Verdict == "REJECT" {
						return dag.CompleteDag(dag.OutcomeFailed)
					}
				}
			}
			return dag.ContinueDag()
		},
	}))
	if err != nil {
		return nil, err
	}
	tasks := map[string]any{}
	for i := 1; i <= 3; i++ {
		if st, ok := res.Status(rName(i)); ok && st == dag.StatusSucceeded {
			v, _ := dag.ResultByName[verdict](res, rName(i))
			tasks[rName(i)] = tSucc(map[string]any{"verdict": v.Verdict})
		}
	}
	return liveRecord("DAG-18", res, tasks, "r1", "r2", "r3"), nil
}

// DAG-19: order-independence. swap flips branch registration order.
func hDAG19(swap bool, dc types.DurableContext) (map[string]any, error) {
	res, err := dag.Dag(dc, "orderindep", func(d *dag.Context) {
		root := dag.Step(d, "root", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) { return 100, nil })
		mkB := func() dag.TaskHandle[int] {
			return dag.Step(d, "b", []dag.AnyHandle{root}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
				v, _ := dag.Get(dp, root)
				return v + 1, nil
			})
		}
		mkC := func() dag.TaskHandle[int] {
			return dag.Step(d, "c", []dag.AnyHandle{root}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
				v, _ := dag.Get(dp, root)
				return v + 2, nil
			})
		}
		var b, c dag.TaskHandle[int]
		if swap {
			c = mkC()
			b = mkB()
		} else {
			b = mkB()
			c = mkC()
		}
		dag.Step(d, "merge", []dag.AnyHandle{b, c}, func(dp dag.Deps, _ dag.StepContext) (int, error) {
			bv, _ := dag.Get(dp, b)
			cv, _ := dag.Get(dp, c)
			return bv + cv, nil
		})
	})
	if err != nil {
		return nil, err
	}
	root, _ := dag.ResultByName[int](res, "root")
	bv, _ := dag.ResultByName[int](res, "b")
	cv, _ := dag.ResultByName[int](res, "c")
	merge, _ := dag.ResultByName[int](res, "merge")
	tasks := map[string]any{
		"root": tSucc(root), "b": tSucc(bv), "c": tSucc(cv), "merge": tSucc(merge),
	}
	return liveRecord("DAG-19", res, tasks, "root", "b", "c", "merge"), nil
}

func sName(i int) string { return "s" + string(rune('0'+i)) }
func tName(i int) string { return "t" + string(rune('0'+i)) }
func rName(i int) string { return "r" + string(rune('0'+i)) }

// ── driver ──────────────────────────────────────────────────────────────

func canon(t *testing.T, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func runH[E any](t *testing.T, name string, h func(E, types.DurableContext) (map[string]any, error), ev E) map[string]any {
	t.Helper()
	runner := dtesting.New(h, nil) // SkipTime defaults on
	res, err := runner.Run(ev)
	if err != nil {
		t.Fatalf("%s: Run: %v", name, err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("%s: expected handler SUCCEEDED, got %s (%s)", name, res.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[map[string]any](res)
	if err != nil {
		t.Fatalf("%s: GetResult: %v", name, err)
	}
	return out
}

func TestDAGConformance(t *testing.T) {
	void := struct{}{}
	results := map[string]any{}

	// Run every applicable scenario.
	actual := map[string]map[string]any{
		"DAG-1":  runH(t, "DAG-1", hDAG1, void),
		"DAG-2":  runH(t, "DAG-2", compensationGraph("DAG-2", true), void),
		"DAG-3":  runH(t, "DAG-3", compensationGraph("DAG-3", false), void),
		"DAG-4":  runH(t, "DAG-4", hDAG4, void),
		"DAG-5":  runH(t, "DAG-5", hDAG5, void),
		"DAG-6":  runH(t, "DAG-6", hDAG6, void),
		"DAG-7":  runH(t, "DAG-7", hDAG7, void),
		"DAG-8":  runH(t, "DAG-8", hDAG8, void),
		"DAG-9":  runH(t, "DAG-9", hDAG9, void),
		"DAG-10": runH(t, "DAG-10", hDAG10, void),
		"DAG-11": runH(t, "DAG-11", hDAG11, void),
		"DAG-12": runH(t, "DAG-12", hDAG12, void),
		"DAG-13": runH(t, "DAG-13", hDAG13, void),
		"DAG-14": runH(t, "DAG-14", hDAG14, void),
		"DAG-15": runH(t, "DAG-15", hDAG15, void),
		"DAG-16": runH(t, "DAG-16", hDAG16, void),
		"DAG-17": runH(t, "DAG-17", hDAG17, void),
		"DAG-18": runH(t, "DAG-18", hDAG18, void),
	}

	// DAG-19: run twice with branch order swapped; the records MUST be
	// byte-identical (observable proof of order-independent name-based IDs).
	dag19a := runH(t, "DAG-19a", hDAG19, false)
	dag19b := runH(t, "DAG-19b", hDAG19, true)
	if canon(t, dag19a) != canon(t, dag19b) {
		t.Errorf("DAG-19 order-independence violated:\n--- run1 ---\n%s\n--- run2 ---\n%s", canon(t, dag19a), canon(t, dag19b))
	}
	actual["DAG-19"] = dag19a

	// Expected records per the catalog. Where Go's DagResult diverges from
	// the catalog it is annotated inline (see DIVERGENCE notes).
	expected := expectedRecords()

	for _, id := range scenarioOrder() {
		got, ok := actual[id]
		if !ok {
			t.Errorf("%s: missing actual record", id)
			continue
		}
		want := expected[id]
		if canon(t, got) != canon(t, want) {
			t.Errorf("%s: record mismatch\n--- got ---\n%s\n--- want ---\n%s", id, canon(t, got), canon(t, want))
		}
		results[id] = got
	}

	// Emit the single key-sorted JSON file (2-space indent, trailing NL).
	if err := os.MkdirAll(filepath.Dir(conformanceOutPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	out := canon(t, results) + "\n"
	if err := os.WriteFile(conformanceOutPath, []byte(out), 0o644); err != nil {
		t.Fatalf("write %s: %v", conformanceOutPath, err)
	}
	t.Logf("wrote %d conformance records to %s", len(results), conformanceOutPath)
}

func scenarioOrder() []string {
	return []string{
		"DAG-1", "DAG-2", "DAG-3", "DAG-4", "DAG-5", "DAG-6", "DAG-7", "DAG-8", "DAG-9", "DAG-10",
		"DAG-11", "DAG-12", "DAG-13", "DAG-14", "DAG-15", "DAG-16", "DAG-17", "DAG-18", "DAG-19",
	}
}

func expectedRecords() map[string]map[string]any {
	all := func(names ...string) map[string]any { return structChecks(true, names...) }
	none := structChecks(false)
	return map[string]map[string]any{
		"DAG-1": record("DAG-1", map[string]any{
			"fetch": tSucc(10), "ta": tSucc(11), "tb": tSucc(20), "merge": tSucc(31),
		}, "ALL_COMPLETED", counts(4, 0, 0, 4), all("fetch", "ta", "tb", "merge"), nil),

		"DAG-2": record("DAG-2", map[string]any{
			"charge": tFail(), "fulfill": tSkip(dag.SkipTriggerRule),
			"refund": tSucc("refunded"), "audit": tSucc("audited"),
		}, "COMPLETED_WITH_FAILURES", counts(2, 1, 1, 4), all("charge", "fulfill", "refund", "audit"), nil),

		"DAG-3": record("DAG-3", map[string]any{
			"charge": tSucc("charged"), "fulfill": tSucc("fulfilled"),
			"refund": tSkip(dag.SkipTriggerRule), "audit": tSucc("audited"),
		}, "ALL_COMPLETED", counts(3, 0, 1, 4), all("charge", "fulfill", "refund", "audit"), nil),

		"DAG-4": record("DAG-4", map[string]any{
			"classify": tSucc("review"), "publish": tSkip(dag.SkipRunIf),
			"review": tSucc("reviewed"), "block": tSkip(dag.SkipRunIf),
		}, "ALL_COMPLETED", counts(2, 0, 2, 4), all("classify", "publish", "review", "block"), nil),

		"DAG-5": record("DAG-5", map[string]any{
			"r_all_success": tSucc("ok"), "r_all_failed": tSkip(dag.SkipTriggerRule),
			"r_all_done": tSucc("ok"), "r_one_success": tSkip(dag.SkipTriggerRule),
			"r_one_failed": tSkip(dag.SkipTriggerRule), "r_none_failed": tSucc("ok"),
		}, "ALL_COMPLETED", counts(3, 0, 3, 6),
			all("r_all_success", "r_all_failed", "r_all_done", "r_one_success", "r_one_failed", "r_none_failed"), nil),

		"DAG-6": record("DAG-6", map[string]any{
			"up_ok": tSucc("ok"), "up_fail": tFail(),
			"c_all_success": tSkip(dag.SkipTriggerRule), "c_all_failed": tSkip(dag.SkipTriggerRule),
			"c_all_done": tSucc("c"), "c_one_success": tSucc("c"),
			"c_one_failed": tSucc("c"), "c_none_failed": tSkip(dag.SkipTriggerRule),
		}, "COMPLETED_WITH_FAILURES", counts(4, 1, 3, 8),
			all("up_ok", "up_fail", "c_all_success", "c_all_failed", "c_all_done", "c_one_success", "c_one_failed", "c_none_failed"), nil),

		"DAG-7": record("DAG-7", map[string]any{
			"u1": tFail(), "u2": tFail(),
			"k_all_success": tSkip(dag.SkipTriggerRule), "k_all_failed": tSucc("k"),
			"k_all_done": tSucc("k"), "k_one_success": tSkip(dag.SkipTriggerRule),
			"k_one_failed": tSucc("k"), "k_none_failed": tSkip(dag.SkipTriggerRule),
		}, "COMPLETED_WITH_FAILURES", counts(3, 2, 3, 8),
			all("u1", "u2", "k_all_success", "k_all_failed", "k_all_done", "k_one_success", "k_one_failed", "k_none_failed"), nil),

		"DAG-8": record("DAG-8", map[string]any{
			"seed": tSucc(1), "gate": tSkip(dag.SkipRunIf),
			"d1": tSkip(dag.SkipTriggerRule), "d2": tSkip(dag.SkipTriggerRule),
			"sink": tSucc("sink"),
		}, "ALL_COMPLETED", counts(2, 0, 3, 5), all("seed", "gate", "d1", "d2", "sink"), nil),

		"DAG-9": record("DAG-9", map[string]any{
			"a": tSucc(2),
			"inner": tSucc(map[string]any{
				"completion_reason": "ALL_COMPLETED",
				"counts":            counts(2, 0, 0, 2),
			}),
			"consume": tSucc(35),
		}, "ALL_COMPLETED", counts(3, 0, 0, 3), all("a", "inner", "consume"), nil),

		"DAG-10": record("DAG-10", map[string]any{}, "ALL_COMPLETED", counts(0, 0, 0, 0), all(), nil),

		"DAG-11": record("DAG-11", map[string]any{}, nil, counts(0, 0, 0, 0), none, "DagCyclicDependencyError"),
		"DAG-12": record("DAG-12", map[string]any{}, nil, counts(0, 0, 0, 0), none, "DagDuplicateTaskError"),
		"DAG-13": record("DAG-13", map[string]any{}, nil, counts(0, 0, 0, 0), none, "DagInvalidTaskNameError"),
		"DAG-14": record("DAG-14", map[string]any{}, nil, counts(0, 0, 0, 0), none, "DagInvalidTaskNameError"),
		"DAG-15": record("DAG-15", map[string]any{}, nil, counts(0, 0, 0, 0), none, "DagInvalidDependencyError"),

		// DAG-16: minSuccessful early-completion. total=5 (registered);
		// s4/s5 never started so are absent from tasks (§9.6) but still
		// count toward total (§2.8).
		"DAG-16": record("DAG-16", map[string]any{
			"s1": tSucc(1), "s2": tSucc(2), "s3": tSucc(3),
		}, "MIN_SUCCESSFUL_REACHED", counts(3, 0, 0, 5), all("s1", "s2", "s3", "s4", "s5"), nil),

		// DAG-17: toleratedFailureCount exceeded. total=4 (registered).
		"DAG-17": record("DAG-17", map[string]any{
			"t1": tFail(), "t2": tFail(),
		}, "FAILURE_TOLERANCE_EXCEEDED", counts(0, 2, 0, 4), all("t1", "t2", "t3", "t4"), nil),

		// DAG-18: custom result-based completion [TS + Go]. total=3
		// (registered); both custom-completion SDKs now agree.
		"DAG-18": record("DAG-18", map[string]any{
			"r1": tSucc(map[string]any{"verdict": "ACCEPT"}),
			"r2": tSucc(map[string]any{"verdict": "REJECT"}),
		}, "CUSTOM_COMPLETION_FAILED", counts(2, 0, 0, 3), all("r1", "r2", "r3"), nil),

		"DAG-19": record("DAG-19", map[string]any{
			"root": tSucc(100), "b": tSucc(101), "c": tSucc(102), "merge": tSucc(203),
		}, "ALL_COMPLETED", counts(4, 0, 0, 4), all("root", "b", "c", "merge"), nil),
	}
}
