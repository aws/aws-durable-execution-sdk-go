package durable_test

import (
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// diamondOut is the smoke handler's JSON-serializable result, projecting a
// few DagResult accessors so the test can assert the aggregate outcome.
type diamondOut struct {
	Merge     int    `json:"merge"`
	Succeeded int    `json:"succeeded"`
	Skipped   int    `json:"skipped"`
	Failed    int    `json:"failed"`
	Total     int    `json:"total"`
	Reason    string `json:"reason"`
}

// diamondHandler runs the classic diamond DAG: fetch(=10) fans out to
// ta(+1)=11 and tb(*2)=20, then merge(a+b)=31 fans in. It exercises typed
// dependency injection via durable.Get across tasks.
func diamondHandler(ctx durable.Context, _ struct{}) (diamondOut, error) {
	res, err := durable.Dag(ctx, "diamond", func(d *durable.DagBuilder) {
		fetch := durable.DagStep(d, "fetch", nil,
			func(_ durable.Deps, _ durable.StepContext) (int, error) { return 10, nil })
		ta := durable.DagStep(d, "ta", []durable.AnyHandle{fetch},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, e := durable.Get(deps, fetch)
				return v + 1, e
			})
		tb := durable.DagStep(d, "tb", []durable.AnyHandle{fetch},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				v, e := durable.Get(deps, fetch)
				return v * 2, e
			})
		durable.DagStep(d, "merge", []durable.AnyHandle{ta, tb},
			func(deps durable.Deps, _ durable.StepContext) (int, error) {
				av, _ := durable.Get(deps, ta)
				bv, _ := durable.Get(deps, tb)
				return av + bv, nil
			})
	})
	if err != nil {
		return diamondOut{}, err
	}
	if err := res.ThrowIfError(); err != nil {
		return diamondOut{}, err
	}
	merge, err := durable.ResultByName[int](res, "merge")
	if err != nil {
		return diamondOut{}, err
	}
	return diamondOut{
		Merge:     merge,
		Succeeded: res.SucceededCount(),
		Skipped:   res.SkippedCount(),
		Failed:    res.FailureCount(),
		Total:     res.TotalCount(),
		Reason:    string(res.CompletionReason()),
	}, nil
}

// TestDagDiamondSmoke proves the ported DAG core runs a diamond graph end to
// end via the local test runner: correct DagResult accessors, and the
// expected CONTEXT/Dag scope op plus one CONTEXT/DagTask op per task.
func TestDagDiamondSmoke(t *testing.T) {
	runner := durabletest.NewLocalRunner(diamondHandler)
	result := runner.RunUntilComplete(t, struct{}{})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, error = %+v", result.Status, result.Error)
	}

	out, err := durabletest.ResultAs[diamondOut](result)
	if err != nil {
		t.Fatalf("ResultAs: %v", err)
	}
	if out.Merge != 31 {
		t.Errorf("merge = %d, want 31", out.Merge)
	}
	if out.Succeeded != 4 || out.Failed != 0 || out.Skipped != 0 || out.Total != 4 {
		t.Errorf("counts = %+v, want succeeded=4 failed=0 skipped=0 total=4", out)
	}
	if out.Reason != "ALL_COMPLETED" {
		t.Errorf("reason = %q, want ALL_COMPLETED", out.Reason)
	}

	// The DAG must materialize a CONTEXT/Dag scope op and one CONTEXT/DagTask
	// op per task, so nested STEP ops always have a valid recorded parent.
	var dagScopes, dagTasks int
	for _, op := range result.OperationsByType("CONTEXT") {
		switch op.SubType {
		case "Dag":
			dagScopes++
		case "DagTask":
			dagTasks++
		}
	}
	if dagScopes != 1 {
		t.Errorf("CONTEXT/Dag scope ops = %d, want 1", dagScopes)
	}
	if dagTasks != 4 {
		t.Errorf("CONTEXT/DagTask ops = %d, want 4", dagTasks)
	}
}
