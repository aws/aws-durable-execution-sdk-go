package durable_test

// DAG behavioral-conformance suite (alpha port).
//
// This ports the SEMANTIC scenarios DAG-1..DAG-19 from the source
// pkg/durable/dag conformance suite onto the alpha DAG core. Each scenario
// runs via the local runner and asserts the actual outcome (per-task
// statuses/results, completion reason, counts, and normalized validation
// error) equals the catalog's expected outcome.
//
// The source's per-language STRUCTURAL entity-ID checks and the external
// JSON-file emission are intentionally NOT ported: alpha materializes DAG
// tasks under positional child-operation IDs (see durable/dag.go), not the
// name-derived DAG_NODE_T_ entity-ID scheme, so those structural assertions
// do not apply. All semantic contract coverage is preserved.

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// ── record builders ────────────────────────────────────────────────────────

func cTSucc(result any) map[string]any {
	return map[string]any{"status": "SUCCEEDED", "result": result, "error_type": nil, "skip_reason": nil}
}

func cTFail() map[string]any {
	return map[string]any{"status": "FAILED", "result": nil, "error_type": "StepError", "skip_reason": nil}
}

func cTSkip(reason durable.SkipReason) map[string]any {
	return map[string]any{"status": "SKIPPED", "result": nil, "error_type": nil, "skip_reason": string(reason)}
}

func cCounts(success, failure, skipped, total int) map[string]any {
	return map[string]any{"success": success, "failure": failure, "skipped": skipped, "total": total}
}

func cRecord(scenario string, tasks map[string]any, reason any, c map[string]any, valErr any) map[string]any {
	return map[string]any{
		"scenario":          scenario,
		"tasks":             tasks,
		"completion_reason": reason,
		"counts":            c,
		"validation_error":  valErr,
	}
}

func cLiveRecord(scenario string, res *durable.DagResult, tasks map[string]any) map[string]any {
	return cRecord(
		scenario, tasks, string(res.CompletionReason()),
		cCounts(res.SucceededCount(), res.FailureCount(), res.SkippedCount(), res.TotalCount()),
		nil,
	)
}

func cSkipReasonOf(res *durable.DagResult, name string) durable.SkipReason {
	return res.Results()[name].SkipReason
}

func cClassifyValidationError(err error) string {
	var ce *durable.DagCyclicDependencyError
	var de *durable.DagDuplicateTaskError
	var ne *durable.DagInvalidTaskNameError
	var pe *durable.DagInvalidDependencyError
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

func cValidationRecord(scenario, token string) map[string]any {
	return cRecord(scenario, map[string]any{}, nil, cCounts(0, 0, 0, 0), token)
}

func cSName(i int) string { return "s" + string(rune('0'+i)) }
func cTName(i int) string { return "t" + string(rune('0'+i)) }
func cRName(i int) string { return "r" + string(rune('0'+i)) }

// ── scenario handlers ──────────────────────────────────────────────────────

// DAG-1: diamond fan-out/in with typed deps.
func hConfDAG1(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "diamond", func(d *durable.DagBuilder) {
		fetch := durable.DagStep(d, "fetch", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 10, nil })
		ta := durable.DagStep(d, "ta", []durable.AnyHandle{fetch}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
			v, _ := durable.Get(dp, fetch)
			return v + 1, nil
		})
		tb := durable.DagStep(d, "tb", []durable.AnyHandle{fetch}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
			v, _ := durable.Get(dp, fetch)
			return v * 2, nil
		})
		durable.DagStep(d, "merge", []durable.AnyHandle{ta, tb}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
			a, _ := durable.Get(dp, ta)
			b, _ := durable.Get(dp, tb)
			return a + b, nil
		})
	})
	if err != nil {
		return nil, err
	}
	fetch, _ := durable.ResultByName[int](res, "fetch")
	ta, _ := durable.ResultByName[int](res, "ta")
	tb, _ := durable.ResultByName[int](res, "tb")
	merge, _ := durable.ResultByName[int](res, "merge")
	tasks := map[string]any{"fetch": cTSucc(fetch), "ta": cTSucc(ta), "tb": cTSucc(tb), "merge": cTSucc(merge)}
	return cLiveRecord("DAG-1", res, tasks), nil
}

// compensationGraph builds the DAG-2/DAG-3 shared graph.
func compensationGraph(scenario string, chargeFails bool) func(dc durable.Context, _ struct{}) (map[string]any, error) {
	return func(dc durable.Context, _ struct{}) (map[string]any, error) {
		res, err := durable.Dag(dc, "order", func(d *durable.DagBuilder) {
			charge := durable.DagStep(d, "charge", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				if chargeFails {
					return "", errors.New("charge failed")
				}
				return "charged", nil
			})
			durable.DagStep(d, "fulfill", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "fulfilled", nil
			}).After(charge)
			durable.DagStep(d, "refund", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "refunded", nil
			}).After(charge).WithTrigger(durable.AllFailed)
			durable.DagStep(d, "audit", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "audited", nil
			}).After(charge).WithTrigger(durable.AllDone)
		})
		if err != nil {
			return nil, err
		}
		tasks := map[string]any{}
		build := func(name string) {
			st, _ := res.Status(name)
			switch st {
			case durable.StatusSucceeded:
				v, _ := durable.ResultByName[string](res, name)
				tasks[name] = cTSucc(v)
			case durable.StatusFailed:
				tasks[name] = cTFail()
			case durable.StatusSkipped:
				tasks[name] = cTSkip(cSkipReasonOf(res, name))
			}
		}
		for _, n := range []string{"charge", "fulfill", "refund", "audit"} {
			build(n)
		}
		return cLiveRecord(scenario, res, tasks), nil
	}
}

// DAG-4: runIf value-branching (exactly one branch runs).
func hConfDAG4(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "branch", func(d *durable.DagBuilder) {
		classify := durable.DagStep(d, "classify", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
			return "review", nil
		})
		mk := func(name, want, out string) {
			durable.DagStep(d, name, []durable.AnyHandle{classify}, func(dp durable.Deps, _ durable.StepContext) (string, error) {
				return out, nil
			}, durable.WithRunIf(func(dp durable.Deps) bool {
				v, _ := durable.Get(dp, classify)
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
	classify, _ := durable.ResultByName[string](res, "classify")
	review, _ := durable.ResultByName[string](res, "review")
	tasks := map[string]any{
		"classify": cTSucc(classify),
		"publish":  cTSkip(cSkipReasonOf(res, "publish")),
		"review":   cTSucc(review),
		"block":    cTSkip(cSkipReasonOf(res, "block")),
	}
	return cLiveRecord("DAG-4", res, tasks), nil
}

func triggerMatrixTasks(res *durable.DagResult) map[string]any {
	tasks := map[string]any{}
	for _, te := range res.Results() {
		name := te.Name
		switch te.Status {
		case durable.StatusSucceeded:
			v, _ := durable.ResultByName[string](res, name)
			tasks[name] = cTSucc(v)
		case durable.StatusFailed:
			tasks[name] = cTFail()
		case durable.StatusSkipped:
			tasks[name] = cTSkip(te.SkipReason)
		}
	}
	return tasks
}

// DAG-5: trigger-rule matrix, empty-upstream row.
func hConfDAG5(dc durable.Context, _ struct{}) (map[string]any, error) {
	rules := []struct {
		name string
		rule durable.TriggerRule
	}{
		{"r_all_success", durable.AllSuccess},
		{"r_all_failed", durable.AllFailed},
		{"r_all_done", durable.AllDone},
		{"r_one_success", durable.AnySuccess},
		{"r_one_failed", durable.AnyFailed},
		{"r_none_failed", durable.NoneFailed},
	}
	res, err := durable.Dag(dc, "matrix_empty", func(d *durable.DagBuilder) {
		for _, r := range rules {
			durable.DagStep(d, r.name, nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "ok", nil
			}, durable.WithTriggerRule(r.rule))
		}
	})
	if err != nil {
		return nil, err
	}
	return cLiveRecord("DAG-5", res, triggerMatrixTasks(res)), nil
}

// DAG-6: trigger-rule matrix, mixed succ/fail upstream.
func hConfDAG6(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "matrix_mixed", func(d *durable.DagBuilder) {
		upOK := durable.DagStep(d, "up_ok", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) { return "ok", nil })
		upFail := durable.DagStep(d, "up_fail", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
			return "", errors.New("boom")
		})
		consumers := []struct {
			name string
			rule durable.TriggerRule
		}{
			{"c_all_success", durable.AllSuccess},
			{"c_all_failed", durable.AllFailed},
			{"c_all_done", durable.AllDone},
			{"c_one_success", durable.AnySuccess},
			{"c_one_failed", durable.AnyFailed},
			{"c_none_failed", durable.NoneFailed},
		}
		for _, c := range consumers {
			durable.DagStep(d, c.name, nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "c", nil
			}).After(upOK, upFail).WithTrigger(c.rule)
		}
	})
	if err != nil {
		return nil, err
	}
	return cLiveRecord("DAG-6", res, triggerMatrixTasks(res)), nil
}

// DAG-7: trigger-rule matrix, all-failed upstream.
func hConfDAG7(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "matrix_allfailed", func(d *durable.DagBuilder) {
		u1 := durable.DagStep(d, "u1", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) { return "", errors.New("boom") })
		u2 := durable.DagStep(d, "u2", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) { return "", errors.New("boom") })
		consumers := []struct {
			name string
			rule durable.TriggerRule
		}{
			{"k_all_success", durable.AllSuccess},
			{"k_all_failed", durable.AllFailed},
			{"k_all_done", durable.AllDone},
			{"k_one_success", durable.AnySuccess},
			{"k_one_failed", durable.AnyFailed},
			{"k_none_failed", durable.NoneFailed},
		}
		for _, c := range consumers {
			durable.DagStep(d, c.name, nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
				return "k", nil
			}).After(u1, u2).WithTrigger(c.rule)
		}
	})
	if err != nil {
		return nil, err
	}
	return cLiveRecord("DAG-7", res, triggerMatrixTasks(res)), nil
}

// DAG-8: skip cascade + "includes SKIPPED" trigger row.
func hConfDAG8(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "cascade", func(d *durable.DagBuilder) {
		seed := durable.DagStep(d, "seed", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
		gate := durable.DagStep(d, "gate", []durable.AnyHandle{seed}, func(_ durable.Deps, _ durable.StepContext) (string, error) {
			return "gate", nil
		}, durable.WithRunIf(func(dp durable.Deps) bool {
			v, _ := durable.Get(dp, seed)
			return v > 100
		}))
		d1 := durable.DagStep(d, "d1", []durable.AnyHandle{gate}, func(_ durable.Deps, _ durable.StepContext) (string, error) {
			return "d1", nil
		})
		durable.DagStep(d, "d2", []durable.AnyHandle{d1}, func(_ durable.Deps, _ durable.StepContext) (string, error) {
			return "d2", nil
		})
		durable.DagStep(d, "sink", nil, func(_ durable.Deps, _ durable.StepContext) (string, error) {
			return "sink", nil
		}).After(gate).WithTrigger(durable.AllDone)
	})
	if err != nil {
		return nil, err
	}
	seed, _ := durable.ResultByName[int](res, "seed")
	sink, _ := durable.ResultByName[string](res, "sink")
	tasks := map[string]any{
		"seed": cTSucc(seed),
		"gate": cTSkip(cSkipReasonOf(res, "gate")),
		"d1":   cTSkip(cSkipReasonOf(res, "d1")),
		"d2":   cTSkip(cSkipReasonOf(res, "d2")),
		"sink": cTSucc(sink),
	}
	return cLiveRecord("DAG-8", res, tasks), nil
}

// DAG-9: nested DAG (result consumed downstream + scope isolation).
func hConfDAG9(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "outer", func(d *durable.DagBuilder) {
		a := durable.DagStep(d, "a", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 2, nil })
		inner := durable.SubDag(d, "inner", []durable.AnyHandle{a}, func(sub *durable.DagBuilder) {
			x := durable.DagStep(sub, "x", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 3, nil })
			durable.DagStep(sub, "y", []durable.AnyHandle{x}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
				v, _ := durable.Get(dp, x)
				return v * 10, nil
			})
		})
		durable.DagStep(d, "consume", []durable.AnyHandle{inner}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
			ir, _ := durable.Get(dp, inner)
			y, _ := durable.ResultByName[int](ir, "y")
			return y + 5, nil
		})
	})
	if err != nil {
		return nil, err
	}
	if _, ok := res.Status("x"); ok {
		return nil, errors.New("scope isolation violated: 'x' visible in outer scope")
	}
	if _, ok := res.Status("y"); ok {
		return nil, errors.New("scope isolation violated: 'y' visible in outer scope")
	}
	a, _ := durable.ResultByName[int](res, "a")
	consume, _ := durable.ResultByName[int](res, "consume")
	innerRes, _ := durable.ResultByName[*durable.DagResult](res, "inner")
	innerNorm := map[string]any{
		"completion_reason": string(innerRes.CompletionReason()),
		"counts":            cCounts(innerRes.SucceededCount(), innerRes.FailureCount(), innerRes.SkippedCount(), innerRes.TotalCount()),
	}
	tasks := map[string]any{
		"a":       cTSucc(a),
		"inner":   cTSucc(innerNorm),
		"consume": cTSucc(consume),
	}
	return cLiveRecord("DAG-9", res, tasks), nil
}

// DAG-10: empty DAG.
func hConfDAG10(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "empty", func(d *durable.DagBuilder) {})
	if err != nil {
		return nil, err
	}
	return cLiveRecord("DAG-10", res, map[string]any{}), nil
}

// DAG-11..15: validation errors.
func hConfDAG11(dc durable.Context, _ struct{}) (map[string]any, error) {
	_, err := durable.Dag(dc, "cycle", func(d *durable.DagBuilder) {
		p := durable.DagStep(d, "p", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 0, nil })
		q := durable.DagStep(d, "q", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 0, nil })
		p.After(q)
		q.After(p)
	})
	if err == nil {
		return nil, errors.New("DAG-11: expected validation error, got nil")
	}
	return cValidationRecord("DAG-11", cClassifyValidationError(err)), nil
}

func hConfDAG12(dc durable.Context, _ struct{}) (map[string]any, error) {
	_, err := durable.Dag(dc, "dup", func(d *durable.DagBuilder) {
		durable.DagStep(d, "dup", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 0, nil })
		durable.DagStep(d, "dup", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-12: expected validation error, got nil")
	}
	return cValidationRecord("DAG-12", cClassifyValidationError(err)), nil
}

func hConfDAG13(dc durable.Context, _ struct{}) (map[string]any, error) {
	_, err := durable.Dag(dc, "dashname", func(d *durable.DagBuilder) {
		durable.DagStep(d, "fetch-data", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 0, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-13: expected validation error, got nil")
	}
	return cValidationRecord("DAG-13", cClassifyValidationError(err)), nil
}

func hConfDAG14(dc durable.Context, _ struct{}) (map[string]any, error) {
	_, err := durable.Dag(dc, "reserved", func(d *durable.DagBuilder) {
		durable.DagStep(d, "DAG_NODE_T_root", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 0, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-14: expected validation error, got nil")
	}
	return cValidationRecord("DAG-14", cClassifyValidationError(err)), nil
}

func hConfDAG15(dc durable.Context, _ struct{}) (map[string]any, error) {
	// Mint a handle in a DIFFERENT DAG scope, then reference it from scope A.
	var foreign durable.AnyHandle
	if _, e := durable.Dag(dc, "sibling", func(sd *durable.DagBuilder) {
		foreign = durable.DagStep(sd, "foreignNode", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 1, nil })
	}); e != nil {
		return nil, e
	}
	_, err := durable.Dag(dc, "scopeA", func(d *durable.DagBuilder) {
		durable.DagStep(d, "t", []durable.AnyHandle{foreign}, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 2, nil })
	})
	if err == nil {
		return nil, errors.New("DAG-15: expected validation error, got nil")
	}
	return cValidationRecord("DAG-15", cClassifyValidationError(err)), nil
}

// DAG-16: minSuccessful threshold early-completion.
func hConfDAG16(dc durable.Context, _ struct{}) (map[string]any, error) {
	minv := 3
	res, err := durable.Dag(dc, "minsucc", func(d *durable.DagBuilder) {
		var prev durable.AnyHandle
		for i := 1; i <= 5; i++ {
			i := i
			var deps []durable.AnyHandle
			if prev != nil {
				deps = []durable.AnyHandle{prev}
			}
			prev = durable.DagStep(d, cSName(i), deps, func(_ durable.Deps, _ durable.StepContext) (int, error) { return i, nil })
		}
	}, durable.WithDagMaxConcurrency(1), durable.WithDagCompletion(durable.DagCompletionConfig{MinSuccessful: &minv}))
	if err != nil {
		return nil, err
	}
	tasks := map[string]any{}
	for i := 1; i <= 5; i++ {
		if st, ok := res.Status(cSName(i)); ok && st == durable.StatusSucceeded {
			v, _ := durable.ResultByName[int](res, cSName(i))
			tasks[cSName(i)] = cTSucc(v)
		}
	}
	return cLiveRecord("DAG-16", res, tasks), nil
}

// DAG-17: toleratedFailureCount exceeded early-completion.
func hConfDAG17(dc durable.Context, _ struct{}) (map[string]any, error) {
	tol := 1
	res, err := durable.Dag(dc, "toltol", func(d *durable.DagBuilder) {
		var prev durable.AnyHandle
		for i := 1; i <= 4; i++ {
			var deps []durable.AnyHandle
			h := durable.DagStep(d, cTName(i), deps, func(_ durable.Deps, _ durable.StepContext) (int, error) {
				return 0, errors.New("boom")
			})
			if prev != nil {
				h.After(prev).WithTrigger(durable.AllDone)
			}
			prev = h
		}
	}, durable.WithDagMaxConcurrency(1), durable.WithDagCompletion(durable.DagCompletionConfig{ToleratedFailureCount: &tol}))
	if err != nil {
		return nil, err
	}
	tasks := map[string]any{}
	for i := 1; i <= 4; i++ {
		if st, ok := res.Status(cTName(i)); ok && st == durable.StatusFailed {
			tasks[cTName(i)] = cTFail()
		}
	}
	return cLiveRecord("DAG-17", res, tasks), nil
}

type confVerdict struct {
	Verdict string `json:"verdict"`
}

// DAG-18: custom result-based completion.
func hConfDAG18(dc durable.Context, _ struct{}) (map[string]any, error) {
	res, err := durable.Dag(dc, "rules", func(d *durable.DagBuilder) {
		verdicts := []string{"ACCEPT", "REJECT", "ACCEPT"}
		var prev durable.AnyHandle
		for i, v := range verdicts {
			v := v
			var deps []durable.AnyHandle
			h := durable.DagStep(d, cRName(i+1), deps, func(_ durable.Deps, _ durable.StepContext) (confVerdict, error) {
				return confVerdict{Verdict: v}, nil
			})
			if prev != nil {
				h.After(prev)
			}
			prev = h
		}
	}, durable.WithDagMaxConcurrency(1), durable.WithDagCompletion(durable.DagCompletionConfig{
		ShouldComplete: func(st durable.DagCompletionStatus) durable.CompletionDecision {
			for _, it := range st.Items {
				if it.Status == durable.StatusSucceeded {
					if v, ok := durable.ResultOf[confVerdict](it); ok && v.Verdict == "REJECT" {
						return durable.CompleteDag(durable.OutcomeFailed)
					}
				}
			}
			return durable.ContinueDag()
		},
	}))
	if err != nil {
		return nil, err
	}
	tasks := map[string]any{}
	for i := 1; i <= 3; i++ {
		if st, ok := res.Status(cRName(i)); ok && st == durable.StatusSucceeded {
			v, _ := durable.ResultByName[confVerdict](res, cRName(i))
			tasks[cRName(i)] = cTSucc(map[string]any{"verdict": v.Verdict})
		}
	}
	return cLiveRecord("DAG-18", res, tasks), nil
}

// DAG-19: order-independence. swap flips branch registration order.
func hConfDAG19(dc durable.Context, swap bool) (map[string]any, error) {
	res, err := durable.Dag(dc, "orderindep", func(d *durable.DagBuilder) {
		root := durable.DagStep(d, "root", nil, func(_ durable.Deps, _ durable.StepContext) (int, error) { return 100, nil })
		mkB := func() durable.TaskHandle[int] {
			return durable.DagStep(d, "b", []durable.AnyHandle{root}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
				v, _ := durable.Get(dp, root)
				return v + 1, nil
			})
		}
		mkC := func() durable.TaskHandle[int] {
			return durable.DagStep(d, "c", []durable.AnyHandle{root}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
				v, _ := durable.Get(dp, root)
				return v + 2, nil
			})
		}
		var b, c durable.TaskHandle[int]
		if swap {
			c = mkC()
			b = mkB()
		} else {
			b = mkB()
			c = mkC()
		}
		durable.DagStep(d, "merge", []durable.AnyHandle{b, c}, func(dp durable.Deps, _ durable.StepContext) (int, error) {
			bv, _ := durable.Get(dp, b)
			cv, _ := durable.Get(dp, c)
			return bv + cv, nil
		})
	})
	if err != nil {
		return nil, err
	}
	root, _ := durable.ResultByName[int](res, "root")
	bv, _ := durable.ResultByName[int](res, "b")
	cv, _ := durable.ResultByName[int](res, "c")
	merge, _ := durable.ResultByName[int](res, "merge")
	tasks := map[string]any{
		"root": cTSucc(root), "b": cTSucc(bv), "c": cTSucc(cv), "merge": cTSucc(merge),
	}
	return cLiveRecord("DAG-19", res, tasks), nil
}

// ── driver ──────────────────────────────────────────────────────────────

func cCanon(t *testing.T, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func cRunH[E any](t *testing.T, name string, h durable.Handler[E, map[string]any], ev E) map[string]any {
	t.Helper()
	runner := durabletest.NewLocalRunner(h)
	res := runner.RunUntilComplete(t, ev)
	if res.Status != durabletest.Succeeded {
		t.Fatalf("%s: expected handler SUCCEEDED, got %s (%+v)", name, res.Status, res.Error)
	}
	out, err := durabletest.ResultAs[map[string]any](res)
	if err != nil {
		t.Fatalf("%s: ResultAs: %v", name, err)
	}
	return out
}

func TestDAGConformance(t *testing.T) {
	void := struct{}{}

	actual := map[string]map[string]any{
		"DAG-1":  cRunH(t, "DAG-1", hConfDAG1, void),
		"DAG-2":  cRunH(t, "DAG-2", compensationGraph("DAG-2", true), void),
		"DAG-3":  cRunH(t, "DAG-3", compensationGraph("DAG-3", false), void),
		"DAG-4":  cRunH(t, "DAG-4", hConfDAG4, void),
		"DAG-5":  cRunH(t, "DAG-5", hConfDAG5, void),
		"DAG-6":  cRunH(t, "DAG-6", hConfDAG6, void),
		"DAG-7":  cRunH(t, "DAG-7", hConfDAG7, void),
		"DAG-8":  cRunH(t, "DAG-8", hConfDAG8, void),
		"DAG-9":  cRunH(t, "DAG-9", hConfDAG9, void),
		"DAG-10": cRunH(t, "DAG-10", hConfDAG10, void),
		"DAG-11": cRunH(t, "DAG-11", hConfDAG11, void),
		"DAG-12": cRunH(t, "DAG-12", hConfDAG12, void),
		"DAG-13": cRunH(t, "DAG-13", hConfDAG13, void),
		"DAG-14": cRunH(t, "DAG-14", hConfDAG14, void),
		"DAG-15": cRunH(t, "DAG-15", hConfDAG15, void),
		"DAG-16": cRunH(t, "DAG-16", hConfDAG16, void),
		"DAG-17": cRunH(t, "DAG-17", hConfDAG17, void),
		"DAG-18": cRunH(t, "DAG-18", hConfDAG18, void),
	}

	// DAG-19: run twice with branch order swapped; records MUST be identical.
	dag19a := cRunH(t, "DAG-19a", hConfDAG19, false)
	dag19b := cRunH(t, "DAG-19b", hConfDAG19, true)
	if cCanon(t, dag19a) != cCanon(t, dag19b) {
		t.Errorf("DAG-19 order-independence violated:\n--- run1 ---\n%s\n--- run2 ---\n%s", cCanon(t, dag19a), cCanon(t, dag19b))
	}
	actual["DAG-19"] = dag19a

	expected := expectedRecords()
	for _, id := range scenarioOrder() {
		got, ok := actual[id]
		if !ok {
			t.Errorf("%s: missing actual record", id)
			continue
		}
		if cCanon(t, got) != cCanon(t, expected[id]) {
			t.Errorf("%s: record mismatch\n--- got ---\n%s\n--- want ---\n%s", id, cCanon(t, got), cCanon(t, expected[id]))
		}
	}
}

func scenarioOrder() []string {
	return []string{
		"DAG-1", "DAG-2", "DAG-3", "DAG-4", "DAG-5", "DAG-6", "DAG-7", "DAG-8", "DAG-9", "DAG-10",
		"DAG-11", "DAG-12", "DAG-13", "DAG-14", "DAG-15", "DAG-16", "DAG-17", "DAG-18", "DAG-19",
	}
}

func expectedRecords() map[string]map[string]any {
	return map[string]map[string]any{
		"DAG-1": cRecord("DAG-1", map[string]any{
			"fetch": cTSucc(10), "ta": cTSucc(11), "tb": cTSucc(20), "merge": cTSucc(31),
		}, "ALL_COMPLETED", cCounts(4, 0, 0, 4), nil),

		"DAG-2": cRecord("DAG-2", map[string]any{
			"charge": cTFail(), "fulfill": cTSkip(durable.SkipTriggerRule),
			"refund": cTSucc("refunded"), "audit": cTSucc("audited"),
		}, "COMPLETED_WITH_FAILURES", cCounts(2, 1, 1, 4), nil),

		"DAG-3": cRecord("DAG-3", map[string]any{
			"charge": cTSucc("charged"), "fulfill": cTSucc("fulfilled"),
			"refund": cTSkip(durable.SkipTriggerRule), "audit": cTSucc("audited"),
		}, "ALL_COMPLETED", cCounts(3, 0, 1, 4), nil),

		"DAG-4": cRecord("DAG-4", map[string]any{
			"classify": cTSucc("review"), "publish": cTSkip(durable.SkipRunIf),
			"review": cTSucc("reviewed"), "block": cTSkip(durable.SkipRunIf),
		}, "ALL_COMPLETED", cCounts(2, 0, 2, 4), nil),

		"DAG-5": cRecord("DAG-5", map[string]any{
			"r_all_success": cTSucc("ok"), "r_all_failed": cTSkip(durable.SkipTriggerRule),
			"r_all_done": cTSucc("ok"), "r_one_success": cTSkip(durable.SkipTriggerRule),
			"r_one_failed": cTSkip(durable.SkipTriggerRule), "r_none_failed": cTSucc("ok"),
		}, "ALL_COMPLETED", cCounts(3, 0, 3, 6), nil),

		"DAG-6": cRecord("DAG-6", map[string]any{
			"up_ok": cTSucc("ok"), "up_fail": cTFail(),
			"c_all_success": cTSkip(durable.SkipTriggerRule), "c_all_failed": cTSkip(durable.SkipTriggerRule),
			"c_all_done": cTSucc("c"), "c_one_success": cTSucc("c"),
			"c_one_failed": cTSucc("c"), "c_none_failed": cTSkip(durable.SkipTriggerRule),
		}, "COMPLETED_WITH_FAILURES", cCounts(4, 1, 3, 8), nil),

		"DAG-7": cRecord("DAG-7", map[string]any{
			"u1": cTFail(), "u2": cTFail(),
			"k_all_success": cTSkip(durable.SkipTriggerRule), "k_all_failed": cTSucc("k"),
			"k_all_done": cTSucc("k"), "k_one_success": cTSkip(durable.SkipTriggerRule),
			"k_one_failed": cTSucc("k"), "k_none_failed": cTSkip(durable.SkipTriggerRule),
		}, "COMPLETED_WITH_FAILURES", cCounts(3, 2, 3, 8), nil),

		"DAG-8": cRecord("DAG-8", map[string]any{
			"seed": cTSucc(1), "gate": cTSkip(durable.SkipRunIf),
			"d1": cTSkip(durable.SkipTriggerRule), "d2": cTSkip(durable.SkipTriggerRule),
			"sink": cTSucc("sink"),
		}, "ALL_COMPLETED", cCounts(2, 0, 3, 5), nil),

		"DAG-9": cRecord("DAG-9", map[string]any{
			"a": cTSucc(2),
			"inner": cTSucc(map[string]any{
				"completion_reason": "ALL_COMPLETED",
				"counts":            cCounts(2, 0, 0, 2),
			}),
			"consume": cTSucc(35),
		}, "ALL_COMPLETED", cCounts(3, 0, 0, 3), nil),

		"DAG-10": cRecord("DAG-10", map[string]any{}, "ALL_COMPLETED", cCounts(0, 0, 0, 0), nil),

		"DAG-11": cRecord("DAG-11", map[string]any{}, nil, cCounts(0, 0, 0, 0), "DagCyclicDependencyError"),
		"DAG-12": cRecord("DAG-12", map[string]any{}, nil, cCounts(0, 0, 0, 0), "DagDuplicateTaskError"),
		"DAG-13": cRecord("DAG-13", map[string]any{}, nil, cCounts(0, 0, 0, 0), "DagInvalidTaskNameError"),
		"DAG-14": cRecord("DAG-14", map[string]any{}, nil, cCounts(0, 0, 0, 0), "DagInvalidTaskNameError"),
		"DAG-15": cRecord("DAG-15", map[string]any{}, nil, cCounts(0, 0, 0, 0), "DagInvalidDependencyError"),

		"DAG-16": cRecord("DAG-16", map[string]any{
			"s1": cTSucc(1), "s2": cTSucc(2), "s3": cTSucc(3),
		}, "MIN_SUCCESSFUL_REACHED", cCounts(3, 0, 0, 5), nil),

		"DAG-17": cRecord("DAG-17", map[string]any{
			"t1": cTFail(), "t2": cTFail(),
		}, "FAILURE_TOLERANCE_EXCEEDED", cCounts(0, 2, 0, 4), nil),

		"DAG-18": cRecord("DAG-18", map[string]any{
			"r1": cTSucc(map[string]any{"verdict": "ACCEPT"}),
			"r2": cTSucc(map[string]any{"verdict": "REJECT"}),
		}, "CUSTOM_COMPLETION_FAILED", cCounts(2, 0, 0, 3), nil),

		"DAG-19": cRecord("DAG-19", map[string]any{
			"root": cTSucc(100), "b": cTSucc(101), "c": cTSucc(102), "merge": cTSucc(203),
		}, "ALL_COMPLETED", cCounts(4, 0, 0, 4), nil),
	}
}
