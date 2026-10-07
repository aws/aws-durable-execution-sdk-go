//go:build durablenocheck

package durable_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestParentContextCheckCompiledOut checks that -tags durablenocheck
// removes the context check: an operation on the parent context inside a
// step body is accepted, and ErrWrongContext is declared but not returned.
func TestParentContextCheckCompiledOut(t *testing.T) {
	handler := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "outer", func(_ durable.StepContext) (string, error) {
			_, err := durable.Step(ctx, "inner-on-parent", func(_ durable.StepContext) (string, error) {
				return "x", nil
			})
			if errors.Is(err, durable.ErrWrongContext) {
				return "", err
			}
			return "accepted", err
		})
	}
	r, err := durabletest.NewLocalRunner(handler).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := durabletest.ResultAs[string](r)
	if err != nil || got != "accepted" {
		t.Fatalf("result = %q, %v; want accepted", got, err)
	}
}
