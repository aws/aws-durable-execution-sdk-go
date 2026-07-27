package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// TestActiveBranchAccountingSiblingContinues verifies that when one branch
// of a parallel execution blocks on a pending operation, sibling branches
// continue and checkpoint their results before the invocation suspends.
func TestActiveBranchAccountingSiblingContinues(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		// Branch A: invoke that is pending.
		futA := Go(ctx, "branch-a", func(childCtx Context) (string, error) {
			return Invoke[string](childCtx, "pending-invoke", "arn:target", "payload")
		})
		// Branch B: step that succeeds immediately.
		futB := Go(ctx, "branch-b", func(childCtx Context) (string, error) {
			return Step(childCtx, "fast-step", func(_ StepContext) (string, error) {
				return "fast-result", nil
			})
		})
		// Wait for both.
		_, _ = futA.Result()
		_, _ = futB.Result()
		return "", nil
	})

	// The invocation should return PENDING (branch A is blocked on an
	// unsettled invoke).
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}

	// Branch B's step should have checkpointed (START + SUCCEED),
	// proving that it continued despite branch A blocking.
	updates := updateBatch(t, fake)
	var stepStart, stepSucceed bool
	for _, u := range updates {
		if aws.ToString(u.Name) == "fast-step" {
			switch u.Action {
			case types.OperationActionStart:
				stepStart = true
			case types.OperationActionSucceed:
				stepSucceed = true
			}
		}
	}
	if !stepStart || !stepSucceed {
		t.Errorf("expected fast-step START+SUCCEED, got start=%v succeed=%v", stepStart, stepSucceed)
	}
}

// TestActiveBranchAccountingNoHang verifies that all blocking paths
// correctly deregister their branch, preventing a hang where the
// invocation never suspends. The test uses a timeout: if the invocation
// hangs, the test fails.
func TestActiveBranchAccountingNoHang(t *testing.T) {
	// Each subtest exercises a different blocking path.
	tests := []struct {
		name    string
		handler Handler[string, string]
	}{
		{
			name: "wait-blocking",
			handler: func(ctx Context, _ string) (string, error) {
				return "", Wait(ctx, "w", time.Second)
			},
		},
		{
			name: "invoke-blocking",
			handler: func(ctx Context, _ string) (string, error) {
				_, err := Invoke[string](ctx, "inv", "arn:target", "x")
				return "", err
			},
		},
		{
			name: "wait-async-blocking",
			handler: func(ctx Context, _ string) (string, error) {
				fut := WaitAsync(ctx, "wa", time.Second)
				_, err := fut.Result()
				return "", err
			},
		},
		{
			name: "invoke-async-blocking",
			handler: func(ctx Context, _ string) (string, error) {
				fut := InvokeAsync[string](ctx, "ia", "arn:target", "x")
				_, err := fut.Result()
				return "", err
			},
		},
		{
			name: "callback-blocking",
			handler: func(ctx Context, _ string) (string, error) {
				cb, err := CreateCallback[string](ctx, "cb")
				if err != nil {
					return "", err
				}
				_, err = cb.Result()
				return "", err
			},
		},
		{
			name: "go-with-wait-blocking",
			handler: func(ctx Context, _ string) (string, error) {
				fut := Go(ctx, "child", func(childCtx Context) (string, error) {
					return "", Wait(childCtx, "inner-wait", time.Second)
				})
				_, err := fut.Result()
				return "", err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeLambda{}
			h := Wrap(tc.handler, withLambdaAPI(fake))

			// Use a goroutine with a deadline to detect hangs.
			done := make(chan string, 1)
			go func() {
				resp, err := h.Invoke(context.Background(), stepPayload(`""`))
				if err != nil {
					done <- "error: " + err.Error()
					return
				}
				done <- string(resp)
			}()

			select {
			case resp := <-done:
				if want := `{"Status":"PENDING"}`; resp != want {
					t.Errorf("response = %s, want %s", resp, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("invocation hung — branch did not deregister")
			}
		})
	}
}

// TestActiveBranchAccountingHandlerSuccessNotBlocked verifies that when
// the handler returns successfully (no pending operations), the invocation
// returns SUCCEEDED even when async branches are registered.
func TestActiveBranchAccountingHandlerSuccessNotBlocked(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake,
		stepPayload(`""`, wireOperation{
			Id:     hashID("1"),
			Type:   string(types.OperationTypeWait),
			Status: "SUCCEEDED",
			Name:   "w",
		}),
		func(ctx Context, _ string) (string, error) {
			// WaitAsync with a pre-completed wait (SUCCEEDED on replay).
			// The async goroutine completes immediately.
			fut := WaitAsync(ctx, "w", time.Second)
			_, _ = fut.Result()
			return "done", nil
		})

	if want := `{"Status":"SUCCEEDED","Result":"\"done\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// TestActiveBranchAccountingConcurrentDeregister exercises the race
// between multiple branches deregistering simultaneously. This test runs
// with -race to catch data races in the branch accounting.
func TestActiveBranchAccountingConcurrentDeregister(t *testing.T) {
	fake := &fakeLambda{}
	const numBranches = 10

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		futures := make([]*Future[string], numBranches)
		for i := range futures {
			futures[i] = Go(ctx, "", func(childCtx Context) (string, error) {
				return "", Wait(childCtx, "w", time.Second)
			})
		}
		// All branches block; wait for one to unwind.
		_, err := futures[0].Result()
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
}

// TestActiveBranchDoubleDeregisterSafe verifies that a branch that
// deregisters multiple times (e.g., preResult + handler unwind) does not
// panic or corrupt state.
func TestActiveBranchDoubleDeregisterSafe(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		// CreateCallback returns a pending callback. Calling Result()
		// fires preResult (deregisters), then the handler unwinds with
		// errSuspendExecution causing another deregister.
		cb, err := CreateCallback[string](ctx, "cb")
		if err != nil {
			return "", err
		}
		_, err = cb.Result()
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Helpers used by multiple tests above ---

// The fakeLambda and invokeStep/stepPayload helpers are defined in
// step_test.go and shared across test files in this package.

// TestAsyncOperationDoesNotBlockCaller verifies that when an async
// operation (WaitAsync, StepAsync, InvokeAsync) enters a pending state, the
// pending flag lands on the operation's own branch, not the caller's
// context: a subsequent operation on the caller's context still claims and
// checkpoints. The seeded operation makes the async op pending on the first
// invocation, so the test waits on the future (deterministic, no sleep) and
// only then runs the follow-up operation on the caller.
func TestAsyncOperationDoesNotBlockCaller(t *testing.T) {
	// afterCheckpointed reports whether a live step named "after" recorded
	// both its START and SUCCEED updates.
	afterCheckpointed := func(t *testing.T, fake *fakeLambda) bool {
		t.Helper()
		var start, succeed bool
		for _, u := range updateBatch(t, fake) {
			if aws.ToString(u.Name) != "after" {
				continue
			}
			switch u.Action {
			case types.OperationActionStart:
				start = true
			case types.OperationActionSucceed:
				succeed = true
			}
		}
		return start && succeed
	}

	runAfterStep := func(ctx Context) error {
		_, err := Step(ctx, "after", func(StepContext) (string, error) {
			return "ok", nil
		})
		return err
	}

	t.Run("WaitAsync", func(t *testing.T) {
		fake := &fakeLambda{}
		resp := invokeStep(t, fake,
			stepPayload(`""`, wireOperation{Id: hashID("1"), Status: "STARTED"}),
			func(ctx Context, _ string) (string, error) {
				fut := WaitAsync(ctx, "wa", time.Second)
				if _, err := fut.Result(); !errors.Is(err, errSuspendExecution) {
					return "", errors.New("async wait did not become pending")
				}
				if err := runAfterStep(ctx); err != nil {
					return "", err
				}
				return "", errSuspendExecution
			})
		if want := `{"Status":"PENDING"}`; resp != want {
			t.Fatalf("response = %s, want %s", resp, want)
		}
		if !afterCheckpointed(t, fake) {
			t.Error("follow-up step did not checkpoint; caller context was blocked by the async wait")
		}
	})

	t.Run("StepAsync", func(t *testing.T) {
		fake := &fakeLambda{}
		resp := invokeStep(t, fake,
			stepPayload(`""`, wireOperation{Id: hashID("1"), Status: "PENDING"}),
			func(ctx Context, _ string) (string, error) {
				fut := StepAsync(ctx, "sa", func(StepContext) (string, error) {
					return "unreached", nil
				})
				if _, err := fut.Result(); !errors.Is(err, errSuspendExecution) {
					return "", errors.New("async step did not become pending")
				}
				if err := runAfterStep(ctx); err != nil {
					return "", err
				}
				return "", errSuspendExecution
			})
		if want := `{"Status":"PENDING"}`; resp != want {
			t.Fatalf("response = %s, want %s", resp, want)
		}
		if !afterCheckpointed(t, fake) {
			t.Error("follow-up step did not checkpoint; caller context was blocked by the async step")
		}
	})

	t.Run("InvokeAsync", func(t *testing.T) {
		fake := &fakeLambda{}
		resp := invokeStep(t, fake,
			stepPayload(`""`, wireOperation{Id: hashID("1"), Status: "STARTED"}),
			func(ctx Context, _ string) (string, error) {
				fut := InvokeAsync[string](ctx, "ia", "arn:target", "x")
				if _, err := fut.Result(); !errors.Is(err, errSuspendExecution) {
					return "", errors.New("async invoke did not become pending")
				}
				if err := runAfterStep(ctx); err != nil {
					return "", err
				}
				return "", errSuspendExecution
			})
		if want := `{"Status":"PENDING"}`; resp != want {
			t.Fatalf("response = %s, want %s", resp, want)
		}
		if !afterCheckpointed(t, fake) {
			t.Error("follow-up step did not checkpoint; caller context was blocked by the async invoke")
		}
	})
}
