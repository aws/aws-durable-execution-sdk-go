package durable_test

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestWaitForConditionRequiresWaitStrategy asserts that WaitForCondition
// rejects a nil ConditionConfig.WaitStrategy at the call, before it runs
// the check and before any checkpoint.
func TestWaitForConditionRequiresWaitStrategy(t *testing.T) {
	var checks atomic.Int64
	var callErr error
	h := func(ctx durable.Context, _ any) (int, error) {
		v, err := durable.WaitForCondition(ctx, "poll",
			func(_ durable.StepContext, s int) (int, error) {
				checks.Add(1)
				return s + 1, nil
			},
			durable.ConditionConfig[int]{InitialState: 0}) // no WaitStrategy
		callErr = err
		return v, err
	}

	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}

	var errType, errMsg string
	if r.Error != nil {
		errType, errMsg = r.Error.Type, r.Error.Message
	}
	t.Logf("status=%s checks=%d invocations=%d errorType=%q error=%q",
		r.Status, checks.Load(), len(r.Invocations), errType, errMsg)

	if got := checks.Load(); got != 0 {
		t.Errorf("check ran %d times, want 0: WaitForCondition must reject a nil WaitStrategy before running the check", got)
	}
	if r.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", r.Status)
	}
	if !strings.Contains(errMsg, "WaitStrategy must not be nil") {
		t.Fatalf("error message = %q, want it to contain %q", errMsg, "WaitStrategy must not be nil")
	}
	const want = `durable: WaitForCondition "poll": ConditionConfig.WaitStrategy must not be nil`
	if callErr == nil || callErr.Error() != want {
		t.Errorf("WaitForCondition error = %v, want %q", callErr, want)
	}
	if n := len(r.Operations); n != 0 {
		t.Errorf("recorded %d operations, want 0: %+v", n, r.Operations)
	}
}

// TestWaitForConditionZeroInitialStateAccepted asserts that the zero value
// of the state type is a valid InitialState: only WaitStrategy is required.
func TestWaitForConditionZeroInitialStateAccepted(t *testing.T) {
	h := func(ctx durable.Context, _ any) (int, error) {
		return durable.WaitForCondition(ctx, "poll",
			func(_ durable.StepContext, s int) (int, error) { return s, nil },
			durable.ConditionConfig[int]{
				WaitStrategy: func(int, int) durable.WaitDecision { return durable.WaitDecision{Continue: false} },
			})
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED (error %+v)", r.Status, r.Error)
	}
}
