// handler.go holds the durable DAG handler, kept separate from main.go's
// runtime wiring so handler_test.go can exercise it via the local test
// runner. It demonstrates the EXPERIMENTAL dag package:
//
//   - a diamond (fetch -> {double, triple} -> merge) using typed deps, and
//   - compensation with trigger rules (charge -> fulfill on success,
//     refund on ALL_FAILED, notify on ALL_DONE).
package main

import (
	"errors"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// Event drives the example: FailCharge makes the payment step fail so the
// compensation branch (refund) runs.
type Event struct {
	Seed       int  `json:"seed"`
	FailCharge bool `json:"failCharge"`
}

// Result reports what the DAG produced.
type Result struct {
	Merged       int    `json:"merged"`
	ChargeStatus string `json:"chargeStatus"`
	Refunded     bool   `json:"refunded"`
	Notified     bool   `json:"notified"`
	Reason       string `json:"reason"`
}

// Handler runs the durable DAG and projects a plain Result.
func Handler(event Event, dc types.DurableContext) (Result, error) {
	res, err := dag.Dag(dc, "order", func(d *dag.Context) {
		// ── diamond ──
		fetch := dag.Step(d, "fetch", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) {
			return event.Seed, nil
		})
		double := dag.Step(d, "double", []dag.AnyHandle{fetch}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
			v, _ := dag.Get(deps, fetch)
			return v * 2, nil
		})
		triple := dag.Step(d, "triple", []dag.AnyHandle{fetch}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
			v, _ := dag.Get(deps, fetch)
			return v * 3, nil
		})
		dag.Step(d, "merge", []dag.AnyHandle{double, triple}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
			dv, _ := dag.Get(deps, double)
			tv, _ := dag.Get(deps, triple)
			return dv + tv, nil
		})

		// ── compensation with trigger rules ──
		charge := dag.Step(d, "charge", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			if event.FailCharge {
				return "", errors.New("card declined")
			}
			return "charged", nil
		})
		dag.Step(d, "fulfill", []dag.AnyHandle{charge}, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "fulfilled", nil
		}) // default ALL_SUCCESS
		dag.Step(d, "refund", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "refunded", nil
		}).DependsOn(charge).WithTrigger(dag.AllFailed)
		dag.Step(d, "notify", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
			return "notified", nil
		}).DependsOn(charge).WithTrigger(dag.AllDone)
	})
	if err != nil {
		return Result{}, err
	}

	out := Result{Reason: res.CompletionReason()}
	out.Merged, _ = dag.ResultByName[int](res, "merge")
	if st, ok := res.Status("charge"); ok {
		out.ChargeStatus = string(st)
	}
	if st, ok := res.Status("refund"); ok && st == dag.StatusSucceeded {
		out.Refunded = true
	}
	if st, ok := res.Status("notify"); ok && st == dag.StatusSucceeded {
		out.Notified = true
	}
	return out, nil
}
