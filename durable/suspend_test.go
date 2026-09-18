package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
			case OperationActionStart:
				stepStart = true
			case OperationActionSucceed:
				stepSucceed = true
			}
		}
	}
	if !stepStart || !stepSucceed {
		t.Errorf("expected fast-step START+SUCCEED, got start=%v succeed=%v", stepStart, stepSucceed)
	}
}

// TestPendingCallbackInGoChildSuspendsWithSiblingProgress verifies that a
// pending callback inside one Go child suspends the invocation while an
// independent sibling Go branch runs to completion and checkpoints. It is the
// callback analogue of TestActiveBranchAccountingSiblingContinues: a blocking
// callback unwinds its own branch the same way a pending invoke does, without
// forcing the sibling to abandon its work, and the invocation still ends
// PENDING.
func TestPendingCallbackInGoChildSuspendsWithSiblingProgress(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		cbBranch := Go(ctx, "cb-branch", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "pending-cb")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})
		fastBranch := Go(ctx, "fast-branch", func(childCtx Context) (string, error) {
			return Step(childCtx, "fast-step", func(_ StepContext) (string, error) {
				return "fast-result", nil
			})
		})
		// Await the fast branch first so its checkpoints are recorded
		// deterministically before the callback branch suspends.
		_, _ = fastBranch.Result()
		_, _ = cbBranch.Result()
		return "", nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	var stepStart, stepSucceed bool
	for _, u := range updates {
		if aws.ToString(u.Name) == "fast-step" {
			switch u.Action {
			case OperationActionStart:
				stepStart = true
			case OperationActionSucceed:
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
		{
			name: "go-with-callback-blocking",
			handler: func(ctx Context, _ string) (string, error) {
				fut := Go(ctx, "child", func(childCtx Context) (string, error) {
					cb, err := CreateCallback[string](childCtx, "inner-cb")
					if err != nil {
						return "", err
					}
					return cb.Result()
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
				resp, err := h(context.Background(), stepPayload(`""`))
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
			Type:   string(OperationTypeWait),
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
			case OperationActionStart:
				start = true
			case OperationActionSucceed:
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

// TestAbandonedGoChildCheckpointRefused verifies that when the invocation
// answers PENDING, an abandoned durable.Go child that is still running
// non-durable work has its subsequent checkpoint refused. The test forces
// the interleaving:
//
//  1. childA holds a pending callback → commits to PENDING
//  2. childB is blocked in non-durable work (channel receive)
//  3. handler unwinds → invocation answers PENDING immediately
//  4. childB unblocks → attempts to checkpoint its step result
//  5. checkpoint is refused (errCheckpointTerminated)
//  6. childB settles its future with errSuspendExecution and signals done
//
// The test cannot pass by scheduler luck: childB's completion channel is
// the synchronization proof that childB received the termination error.
// The PENDING response is returned without waiting for childB, proving the
// handler does not join orphaned children.
func TestAbandonedGoChildCheckpointRefused(t *testing.T) {
	fake := &fakeLambda{}

	// childBDone: closed by childB after it observes checkpoint refusal.
	childBDone := make(chan struct{})
	// childBGate: released by the test after PENDING is confirmed.
	childBGate := make(chan struct{})
	// childBErr: the error childB's step returned.
	var childBErr error

	h := Wrap[string, string](func(ctx Context, _ string) (string, error) {
		// childA: pending callback → drives suspension.
		childA := Go(ctx, "child-a", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "pending-cb")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})

		// childB: blocked in non-durable work; will attempt a step
		// checkpoint after being released.
		_ = Go(ctx, "child-b", func(childCtx Context) (string, error) {
			defer close(childBDone)
			// Block until the test releases us (after PENDING).
			<-childBGate
			// Attempt a step — the START checkpoint should be refused.
			_, err := Step(childCtx, "orphan-step", func(_ StepContext) (string, error) {
				return "orphan-result", nil
			})
			childBErr = err
			return "", err
		})

		// Await childA — its pending callback suspends the invocation.
		_, _ = childA.Result()
		return "", errSuspendExecution
	}, withLambdaAPI(fake))

	// Run the handler with a deadline.
	type invokeResult struct {
		resp []byte
		err  error
	}
	done := make(chan invokeResult, 1)
	go func() {
		resp, err := h(context.Background(), stepPayload(`""`))
		done <- invokeResult{resp: resp, err: err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Invoke error: %v", r.err)
		}
		if want := `{"Status":"PENDING"}`; string(r.resp) != want {
			t.Fatalf("response = %s, want %s", string(r.resp), want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("invocation hung — PENDING blocked on abandoned child")
	}

	// At this point PENDING has been returned; childB is still blocked.
	// Release it and wait for it to observe checkpoint refusal.
	close(childBGate)

	select {
	case <-childBDone:
		// childB completed — verify it received errSuspendExecution.
	case <-time.After(5 * time.Second):
		t.Fatal("childB did not complete after checkpoint refusal")
	}

	// childB's step must have received errSuspendExecution (translated
	// from errCheckpointTerminated at the START checkpoint).
	if !errors.Is(childBErr, errSuspendExecution) {
		t.Errorf("childB step error = %v, want errSuspendExecution", childBErr)
	}

	// Verify no checkpoint was recorded for the orphan step: snapshot
	// fakeLambda state under its mutex.
	fake.mu.Lock()
	batches := make([][]OperationUpdate, len(fake.gotUpdateBatches))
	copy(batches, fake.gotUpdateBatches)
	fake.mu.Unlock()

	for _, batch := range batches {
		for _, u := range batch {
			if aws.ToString(u.Name) == "orphan-step" {
				t.Errorf("orphan-step checkpoint was recorded (action=%s); expected refusal", u.Action)
			}
		}
	}
}

// TestSuspendSignalSettlesFutureRegisteredDuringFire verifies that a
// future registered concurrently with fire is always settled: either by
// fire's drain pass, or immediately by registerFuture when it observes
// that fire has started. Each iteration pre-registers many futures so the
// drain pass is long enough for a concurrent registration to land while
// fire is still settling.
func TestSuspendSignalSettlesFutureRegisteredDuringFire(t *testing.T) {
	const iterations = 3000
	lost := 0
	for range iterations {
		s := newSuspendSignal()
		for range 200 {
			registerFuture(s, newFuture[int]())
		}
		late := newFuture[int]()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); s.fire() }()
		go func() { defer wg.Done(); registerFuture(s, late) }()
		wg.Wait()
		if !late.settled() {
			lost++
		}
	}
	if lost > 0 {
		t.Errorf("%d/%d late-registered futures were never settled after fire()", lost, iterations)
	}
}

// TestSuspendSignalLateFutureSettlesWithSuspendError verifies that a future
// registered after fire has completed is settled with errSuspendExecution,
// the same outcome the drain pass gives futures registered before fire.
func TestSuspendSignalLateFutureSettlesWithSuspendError(t *testing.T) {
	s := newSuspendSignal()
	s.fire()
	late := newFuture[int]()
	registerFuture(s, late)
	if !late.settled() {
		t.Fatal("future registered after fire() was not settled")
	}
	_, err := late.Result()
	if !errors.Is(err, errSuspendExecution) {
		t.Fatalf("late future error = %v, want errSuspendExecution", err)
	}
}

// TestAbandonHandleChainInheritsAbandonment verifies that a handle minted
// beneath an already-abandoned handle is abandoned from the start, and that
// a commitment against it records nothing. This closes the window in which
// a nested batch is minted after its enclosing batch has retired: the
// nested handle needs no registration to observe the retirement.
func TestAbandonHandleChainInheritsAbandonment(t *testing.T) {
	s := newSuspendSignal()
	outer := newAbandonHandle(nil)
	s.retireCommitment(outer)
	if !outer.abandoned() {
		t.Fatal("retired handle is not abandoned")
	}
	inner := newAbandonHandle(outer)
	if !inner.abandoned() {
		t.Fatal("handle minted beneath a retired handle is not abandoned")
	}
	// Keep a branch active so a commitment cannot fire the signal; the
	// assertion is about what is recorded, not about firing.
	s.registerBranch()
	defer s.deregisterBranch()
	s.commitPending(inner)
	if s.committed() {
		t.Fatal("commitment against a handle beneath a retired handle was recorded")
	}
	if len(s.branchCommits) != 0 {
		t.Fatalf("branchCommits = %v, want empty", s.branchCommits)
	}
}

// TestRetireCommitmentCascadesThroughHandleChain verifies that retiring a
// handle removes commitments recorded against every handle beneath it,
// reached through the parent chain alone, while a commitment under a
// sibling subtree stands. No registry links the handles: a nested batch
// that has already returned, and whose handle is held only by a
// durable.Go branch it launched, is still retired by an enclosing batch.
func TestRetireCommitmentCascadesThroughHandleChain(t *testing.T) {
	s := newSuspendSignal()
	s.registerBranch()
	defer s.deregisterBranch()

	root := newAbandonHandle(nil)
	inner := newAbandonHandle(root)
	leaf := newAbandonHandle(inner)
	sibling := newAbandonHandle(nil)

	s.commitPending(leaf)
	s.commitPending(inner)
	if !s.committed() {
		t.Fatal("commitments under live handles were not recorded")
	}
	s.retireCommitment(root)
	if s.committed() {
		t.Fatalf("commitments beneath the retired handle stand: %v", s.branchCommits)
	}
	if !leaf.abandoned() || !inner.abandoned() {
		t.Fatal("handles beneath the retired handle do not report abandoned")
	}
	if sibling.abandoned() {
		t.Fatal("sibling handle reports abandoned")
	}

	s.commitPending(sibling)
	if !s.committed() {
		t.Fatal("commitment under a sibling subtree was not recorded")
	}
	s.commitPending(leaf)
	if n := s.branchCommits[leaf]; n != 0 {
		t.Fatalf("late commitment against a retired subtree recorded %d", n)
	}
}
