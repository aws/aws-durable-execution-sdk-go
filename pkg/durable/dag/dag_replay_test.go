package dag_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/dag"
	dtesting "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/testing"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

type dagOut struct {
	Merged   int
	Success  int
	Failure  int
	Skipped  int
	Reason   string
}

// TestDag_DiamondSucceeds runs a fetch -> {a,b} -> merge diamond end to end
// on the local runner and asserts the aggregate result.
func TestDag_DiamondSucceeds(t *testing.T) {
	var fetchRuns, mergeRuns int32

	handler := func(_ struct{}, dc types.DurableContext) (dagOut, error) {
		res, err := dag.Dag(dc, "etl", func(d *dag.Context) {
			fetch := dag.Step(d, "fetch", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) {
				atomic.AddInt32(&fetchRuns, 1)
				return 10, nil
			})
			a := dag.Step(d, "a", []dag.AnyHandle{fetch}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
				v, _ := dag.Get(deps, fetch)
				return v + 1, nil
			})
			b := dag.Step(d, "b", []dag.AnyHandle{fetch}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
				v, _ := dag.Get(deps, fetch)
				return v + 2, nil
			})
			dag.Step(d, "merge", []dag.AnyHandle{a, b}, func(deps dag.Deps, _ dag.StepContext) (int, error) {
				atomic.AddInt32(&mergeRuns, 1)
				av, _ := dag.Get(deps, a)
				bv, _ := dag.Get(deps, b)
				return av + bv, nil
			})
		})
		if err != nil {
			return dagOut{}, err
		}
		if e := res.Err(); e != nil {
			return dagOut{}, e
		}
		merge, _ := dag.ResultByName[int](res, "merge")
		return dagOut{Merged: merge, Success: res.SuccessCount(), Failure: res.FailureCount(), Skipped: res.SkippedCount(), Reason: res.CompletionReason()}, nil
	}

	runner := dtesting.New(handler, nil)
	result, err := runner.Run(struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED, got %s (%s)", result.GetStatus(), msg)
	}
	out, err := dtesting.GetResult[dagOut](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	// a=11, b=12 => merge=23
	if out.Merged != 23 {
		t.Fatalf("merge=%d want 23", out.Merged)
	}
	if out.Success != 4 || out.Failure != 0 || out.Reason != dag.AllCompleted {
		t.Fatalf("unexpected aggregate: %+v", out)
	}
}

// TestDag_TaskFailureReportedInResult asserts a task failure surfaces via
// res.Err() (not the Dag error return) and cascades skips.
func TestDag_TaskFailureReportedInResult(t *testing.T) {
	type out struct {
		DagErr   bool
		Failures int
		Skips    int
		Reason   string
	}
	handler := func(_ struct{}, dc types.DurableContext) (out, error) {
		res, err := dag.Dag(dc, "wf", func(d *dag.Context) {
			root := dag.Step(d, "root", nil, func(_ dag.Deps, _ dag.StepContext) (int, error) {
				return 0, errors.New("boom")
			})
			dag.Step(d, "downstream", []dag.AnyHandle{root}, func(_ dag.Deps, _ dag.StepContext) (int, error) {
				return 1, nil
			})
		})
		if err != nil {
			return out{}, err // registration/validation error path (not expected)
		}
		return out{DagErr: res.Err() != nil, Failures: res.FailureCount(), Skips: res.SkippedCount(), Reason: res.CompletionReason()}, nil
	}
	runner := dtesting.New(handler, nil)
	result, err := runner.Run(struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("handler should succeed (task failures are in-result), got %s (%s)", result.GetStatus(), msg)
	}
	o, err := dtesting.GetResult[out](result)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if !o.DagErr || o.Failures != 1 || o.Skips != 1 || o.Reason != dag.CompletedWithFailures {
		t.Fatalf("unexpected: %+v", o)
	}
}

// TestDag_WaitSuspendResume exercises the suspend protocol: a Wait task
// between two steps suspends the invocation, and the SkipTime runner drives
// it to completion across a resume.
func TestDag_WaitSuspendResume(t *testing.T) {
	var beforeRuns, afterRuns int32
	handler := func(_ struct{}, dc types.DurableContext) (string, error) {
		res, err := dag.Dag(dc, "timed", func(d *dag.Context) {
			before := dag.Step(d, "before", nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				atomic.AddInt32(&beforeRuns, 1)
				return "b", nil
			})
			pause := dag.Wait(d, "pause", []dag.AnyHandle{before}, types.Duration{Seconds: 5})
			dag.Step(d, "after", []dag.AnyHandle{pause}, func(_ dag.Deps, _ dag.StepContext) (string, error) {
				atomic.AddInt32(&afterRuns, 1)
				return "a", nil
			})
		})
		if err != nil {
			return "", err
		}
		if e := res.Err(); e != nil {
			return "", e
		}
		return res.CompletionReason(), nil
	}
	runner := dtesting.New(handler, nil) // SkipTime defaults on
	result, err := runner.Run(struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := result.GetError()
		t.Fatalf("expected SUCCEEDED after wait resolves, got %s (%s)", result.GetStatus(), msg)
	}
	// Side effects counted once despite suspend/resume replays.
	if beforeRuns != 1 || afterRuns != 1 {
		t.Fatalf("side effects not once: before=%d after=%d", beforeRuns, afterRuns)
	}
}

// TestDag_CustomCompletion exercises a results-aware custom predicate that
// fails the DAG when any rule returns REJECT.
func TestDag_CustomCompletion(t *testing.T) {
	handler := func(reject bool, dc types.DurableContext) (string, error) {
		res, err := dag.Dag(dc, "rules", func(d *dag.Context) {
			for i := 0; i < 4; i++ {
				i := i
				dag.Step(d, ruleName(i), nil, func(_ dag.Deps, _ dag.StepContext) (string, error) {
					if reject && i == 2 {
						return "REJECT", nil
					}
					return "OK", nil
				})
			}
		},
			dag.WithMaxConcurrency(1),
			dag.WithCompletion(dag.DagCompletionConfig{
				ShouldComplete: func(st dag.DagCompletionStatus) dag.CompletionDecision {
					for _, it := range st.Items {
						if it.Status == dag.StatusSucceeded {
							if v, ok := dag.ResultOf[string](it); ok && v == "REJECT" {
								return dag.CompleteDag(dag.OutcomeFailed)
							}
						}
					}
					return dag.ContinueDag()
				},
			}),
		)
		if err != nil {
			return "", err
		}
		return res.CompletionReason(), nil
	}

	runner := dtesting.New(handler, nil)
	res, err := runner.Run(true)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.GetStatus() != types.ExecutionStatusSucceeded {
		msg, _ := res.GetError()
		t.Fatalf("expected handler SUCCEEDED, got %s (%s)", res.GetStatus(), msg)
	}
	reason, err := dtesting.GetResult[string](res)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if reason != dag.CustomCompletionFailed {
		t.Fatalf("reason=%q want CUSTOM_COMPLETION_FAILED", reason)
	}
}

func ruleName(i int) string {
	return "rule_" + string(rune('0'+i))
}

var _ = durable.Handler[struct{}, string](nil)
