package durable_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

type Order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

// TestTodayParallelAnyIsLossy shows the only typed fan-out with concurrency
// control before ParallelMixed, durable.Parallel[O], forces one result type
// O. Running it as Parallel[any] over branches that are naturally a struct,
// an int, and a string returns map[string]interface{} for the struct and
// float64 for the int. TestParallelMixedReturnsBranchTypes is its positive
// counterpart.
func TestTodayParallelAnyIsLossy(t *testing.T) {
	handler := func(ctx durable.Context, _ struct{}) ([]string, error) {
		res, err := durable.Parallel(ctx, "fan", []durable.Branch[any]{
			{Name: "order", Func: func(_ durable.Context) (any, error) { return Order{ID: "A1", Total: 42}, nil }},
			{Name: "count", Func: func(_ durable.Context) (any, error) { return 7, nil }},
			{Name: "label", Func: func(_ durable.Context) (any, error) { return "hello", nil }},
		})
		if err != nil {
			return nil, err
		}
		types := make([]string, 0, 3)
		for _, v := range res.Results() {
			types = append(types, fmt.Sprintf("%T", v))
		}
		return types, nil
	}

	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
	got, err := durabletest.ResultAs[[]string](r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Parallel[any] concrete types = %v", got)
}
