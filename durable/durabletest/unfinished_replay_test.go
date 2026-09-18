package durabletest_test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestUnfinishedReplayInLastBranchReturns is the regression test for a
// synchronous await of an operation that has no checkpoint inside a child
// context whose result is already recorded. The awaiting goroutine is the
// only active branch and no pending commitment exists, so nothing can fire
// the suspend signal. The invocation must still return within a bound, and
// the execution must fail with an error that names the operation instead
// of stalling until the invocation deadline.
func TestUnfinishedReplayInLastBranchReturns(t *testing.T) {
	big := strings.Repeat("x", 300*1024) // forces replay-children mode on the child
	var invocations int32

	h := func(ctx durable.Context, _ any) (string, error) {
		n := atomic.AddInt32(&invocations, 1)
		out, err := durable.RunInChildContext(ctx, "big", func(c durable.Context) (string, error) {
			if n > 1 {
				// Replay of a SUCCEEDED child reaches an operation with no
				// checkpoint. This is deliberate nondeterminism.
				if _, serr := durable.Step(c, "extra", func(durable.StepContext) (int, error) {
					return 1, nil
				}); serr != nil {
					return "", serr
				}
			}
			return big, nil
		})
		if err != nil {
			return "", err
		}
		if err := durable.Wait(ctx, "w", time.Second); err != nil {
			return "", err
		}
		return out[:3], nil
	}

	r := durabletest.NewLocalRunner(h)
	done := make(chan *durabletest.TestResult, 1)
	go func() {
		done <- r.RunUntilComplete(t, nil, durabletest.WithMaxInvocations(4))
	}()
	var res *durabletest.TestResult
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("invocation never returned: parked branch with no pending commitment and no other active branch")
	}

	if res.CapReached {
		t.Fatalf("invocation cap reached: status=%v", res.Status)
	}
	if res.Status != durabletest.Failed || res.Error == nil {
		t.Fatalf("status = %v, want FAILED with an error", res.Status)
	}
	// The determinism error escaped the child body while it re-executed,
	// so it surfaces as a child failure, the same shape as on the first
	// run. Its message still names the unfinished operation.
	if res.Error.Type != "ChildContextError" {
		t.Errorf("error type = %q, want ChildContextError", res.Error.Type)
	}
	if !strings.Contains(res.Error.Message, `name "extra"`) {
		t.Errorf("error message %q does not name the unfinished step", res.Error.Message)
	}
	if got := atomic.LoadInt32(&invocations); got != 2 {
		t.Errorf("invocations = %d, want 2 (live run, then the failing replay)", got)
	}
}
