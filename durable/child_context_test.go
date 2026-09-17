package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// --- Future tests ---

func TestFutureSettleOnce(t *testing.T) {
	tests := []struct {
		name      string
		first     func(f *Future[string])
		second    func(f *Future[string])
		wantValue string
		wantErr   bool
	}{
		{
			name:      "success then error",
			first:     func(f *Future[string]) { f.settle("hello", nil) },
			second:    func(f *Future[string]) { f.settle("", errors.New("late")) },
			wantValue: "hello",
			wantErr:   false,
		},
		{
			name:      "error then success",
			first:     func(f *Future[string]) { f.settle("", errors.New("first")) },
			second:    func(f *Future[string]) { f.settle("late", nil) },
			wantValue: "",
			wantErr:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFuture[string]()
			tc.first(f)
			tc.second(f)

			v, err := f.Result()
			if v != tc.wantValue {
				t.Errorf("Result() value = %q, want %q", v, tc.wantValue)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("Result() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestFutureConcurrentSettle(t *testing.T) {
	// Multiple goroutines racing to settle: exactly one wins.
	f := newFuture[int]()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			f.settle(n, nil)
		}(i)
	}
	wg.Wait()

	v, err := f.Result()
	if err != nil {
		t.Fatalf("Result() error = %v", err)
	}
	if v < 0 || v >= 100 {
		t.Errorf("Result() = %d, want [0,100)", v)
	}

	// Re-read: same value.
	v2, err2 := f.Result()
	if v2 != v || err2 != err {
		t.Errorf("second Result() = (%d,%v), want (%d,%v)", v2, err2, v, err)
	}
}

func TestFutureResultBlocks(t *testing.T) {
	f := newFuture[string]()
	done := make(chan struct{})
	go func() {
		_, _ = f.Result()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Result() returned before settle")
	case <-time.After(20 * time.Millisecond):
	}

	f.settle("ok", nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Result() did not return after settle")
	}
}

func TestFutureSettled(t *testing.T) {
	f := newFuture[int]()
	if f.settled() {
		t.Fatal("settled() = true before settle")
	}

	f.settle(42, nil)

	if !f.settled() {
		t.Fatal("settled() = false after settle")
	}
}

// TestFutureSettledDoesNotRunPreResult verifies that settled is a passive
// observation: unlike Result, it must not fire the pre-result hook that
// commits a pending callback future to suspension.
func TestFutureSettledDoesNotRunPreResult(t *testing.T) {
	f := newFuture[int]()
	fired := false
	f.preResult = func() { fired = true }

	if f.settled() {
		t.Fatal("settled() = true before settle")
	}
	if fired {
		t.Fatal("settled() ran the pre-result hook")
	}
}

// TestFutureAndCallbackExposeNoChannel guards the API decision that the
// only ways to wait on a Future or Callback are Result and the combinators.
// A select over settle signals is not replay-safe, so no method may hand
// out a channel.
func TestFutureAndCallbackExposeNoChannel(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(&Future[int]{}),
		reflect.TypeOf(&Callback[int]{}),
	} {
		if _, ok := typ.MethodByName("Done"); ok {
			t.Errorf("%s has an exported Done method", typ)
		}
		for i := range typ.NumMethod() {
			m := typ.Method(i)
			for j := range m.Type.NumOut() {
				if m.Type.Out(j).Kind() == reflect.Chan {
					t.Errorf("%s.%s returns a channel", typ, m.Name)
				}
			}
		}
	}
}

func TestNewFailedFuture(t *testing.T) {
	f := newFailedFuture[string](errors.New("boom"))
	v, err := f.Result()
	if v != "" {
		t.Errorf("value = %q, want empty", v)
	}
	if err == nil || err.Error() != "boom" {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestNewSettledFuture(t *testing.T) {
	f := newSettledFuture("hello", nil)
	v, err := f.Result()
	if v != "hello" || err != nil {
		t.Errorf("Result() = (%q, %v), want (hello, nil)", v, err)
	}
}

// --- Suspend signal + future registration tests ---

func TestSuspendSignalSettlesRegisteredFutures(t *testing.T) {
	sig := newSuspendSignal()
	f1 := newFuture[string]()
	f2 := newFuture[int]()
	registerFuture(sig, f1)
	registerFuture(sig, f2)

	sig.fire()

	_, err1 := f1.Result()
	if !errors.Is(err1, errSuspendExecution) {
		t.Errorf("f1 err = %v, want errSuspendExecution", err1)
	}
	_, err2 := f2.Result()
	if !errors.Is(err2, errSuspendExecution) {
		t.Errorf("f2 err = %v, want errSuspendExecution", err2)
	}
}

func TestSuspendSignalAlreadyFiredSettlesLateRegistration(t *testing.T) {
	sig := newSuspendSignal()
	sig.fire()

	f := newFuture[string]()
	registerFuture(sig, f)

	_, err := f.Result()
	if !errors.Is(err, errSuspendExecution) {
		t.Errorf("err = %v, want errSuspendExecution", err)
	}
}

func TestSuspendSignalDoesNotSettleAlreadySettledFuture(t *testing.T) {
	sig := newSuspendSignal()
	f := newFuture[string]()
	registerFuture(sig, f)

	// Settle normally before suspension fires.
	f.settle("normal", nil)
	sig.fire()

	v, err := f.Result()
	if v != "normal" || err != nil {
		t.Errorf("Result() = (%q, %v), want (normal, nil)", v, err)
	}
}

// --- RunInChildContextAsync tests ---

// childPayload builds an invocation payload with child-context operations.
func childPayload(event string, ops ...wireOperation) []byte {
	all := append([]wireOperation{{
		Id:               "exec-op",
		Status:           "STARTED",
		ExecutionDetails: &wireExecutionDetails{InputPayload: event},
	}}, ops...)
	in := invocationInput{
		DurableExecutionArn:   "arn:test",
		CheckpointToken:       "token-0",
		InitialExecutionState: initialExecutionState{Operations: all},
	}
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	return b
}

// checkpointedChild is a shorthand for a child-context operation in the
// embedded state page, keyed by its wire (hashed) ID.
func checkpointedChild(positionalID, status string, details *wireContextDetails) wireOperation {
	return wireOperation{
		Id:             hashID(positionalID),
		Status:         status,
		ContextDetails: details,
	}
}

func TestReplayChildrenUnfinishedStepDoesNotExecute(t *testing.T) {
	// A step that was still in flight (STARTED) when its context's result
	// was recorded must not re-execute during ReplayChildren replay, and
	// must not checkpoint anything.
	fake := &fakeLambda{}
	executed := false
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"kept"`},
		},
		wireOperation{Id: hashID("1-2"), Status: "STARTED"},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			out, err := Step(childCtx, "s1", func(StepContext) (string, error) {
				t.Error("terminal step body must not execute on replay")
				return "", nil
			})
			// Fire-and-forget: the context's result never depended on
			// this step, so it can be unfinished in the checkpoint log.
			StepAsync(childCtx, "s2", func(StepContext) (string, error) {
				executed = true
				return "side-effect", nil
			})
			return out, err
		})
	})

	if executed {
		t.Error("unfinished step body executed during ReplayChildren replay")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"kept\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replay sent %d updates, want 0", n)
	}
}

func TestReplayChildrenMissingStepDoesNotExecute(t *testing.T) {
	// A step with no checkpoint at all inside a succeeded context must not
	// execute during ReplayChildren replay: it never started before the
	// context's result was recorded.
	fake := &fakeLambda{}
	executed := false
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"kept"`},
		},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			out, err := Step(childCtx, "s1", func(StepContext) (string, error) {
				return "", nil
			})
			StepAsync(childCtx, "s2", func(StepContext) (string, error) {
				executed = true
				return "side-effect", nil
			})
			return out, err
		})
	})

	if executed {
		t.Error("missing step body executed during ReplayChildren replay")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"kept\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replay sent %d updates, want 0", n)
	}
}

func TestReplayChildrenPendingStepDoesNotForcePending(t *testing.T) {
	// A step with a scheduled retry (PENDING) inside a succeeded context
	// must not commit the invocation to PENDING during ReplayChildren
	// replay: the context's result is already recorded.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"kept"`},
		},
		wireOperation{
			Id:          hashID("1-2"),
			Status:      "PENDING",
			StepDetails: &wireStepDetails{Attempt: 1},
		},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			out, err := Step(childCtx, "s1", func(StepContext) (string, error) {
				return "", nil
			})
			StepAsync(childCtx, "s2", func(StepContext) (string, error) {
				return "", errors.New("still failing")
			})
			return out, err
		})
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"kept\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestReplayChildrenUnfinishedInvokeDoesNotForcePending(t *testing.T) {
	// An unresolved chained invoke inside a succeeded context must not
	// suspend the invocation during ReplayChildren replay.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"kept"`},
		},
		wireOperation{Id: hashID("1-2"), Status: "STARTED"},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			out, err := Step(childCtx, "s1", func(StepContext) (string, error) {
				return "", nil
			})
			InvokeAsync[string](childCtx, "inv", "target-fn", "payload")
			return out, err
		})
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"kept\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replay sent %d updates, want 0", n)
	}
}

func TestReplayChildrenUnresolvedCallbackDoesNotForcePending(t *testing.T) {
	// An unresolved callback inside a succeeded context must not commit
	// the invocation to PENDING during ReplayChildren replay, even when
	// its result is awaited through a combinator drain.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:              hashID("1-1"),
			Status:          "STARTED",
			CallbackDetails: &wireCallbackDetails{CallbackId: "cb-1"},
		},
		wireOperation{
			Id:          hashID("1-2"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"kept"`},
		},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb")
			if err != nil {
				return "", err
			}
			if got := cb.ID(); got != "cb-1" {
				t.Errorf("callback ID = %q, want cb-1", got)
			}
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return "", nil
			})
		})
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"kept\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replay sent %d updates, want 0", n)
	}
}

func TestReplayChildrenUnfinishedOpThenLiveSuspension(t *testing.T) {
	// After reconstructing a context that contains an unfinished
	// operation, the execution continues live; a subsequent wait must
	// still be able to suspend the invocation.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"kept"`},
		},
		wireOperation{Id: hashID("1-2"), Status: "STARTED"},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		out, err := RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			inner, ierr := Step(childCtx, "s1", func(StepContext) (string, error) {
				return "", nil
			})
			StepAsync(childCtx, "s2", func(StepContext) (string, error) {
				t.Error("unfinished step body must not execute")
				return "", nil
			})
			return inner, ierr
		})
		if err != nil {
			return "", err
		}
		if err := Wait(ctx, "settle", time.Hour); err != nil {
			return "", err
		}
		return out, nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRunInChildContextAsyncSuccess(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"hello"`), func(ctx Context, event string) (string, error) {
		fut := RunInChildContextAsync(ctx, "child1", func(childCtx Context) (string, error) {
			return Step(childCtx, "inner", func(StepContext) (string, error) {
				return event + "-done", nil
			})
		})
		result, err := fut.Result()
		if err != nil {
			return "", err
		}
		return result, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"hello-done\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRunInChildContextAsyncReplaySucceeded(t *testing.T) {
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: `"replayed"`}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "child1", func(childCtx Context) (string, error) {
			t.Fatal("fn should not execute on replay")
			return "", nil
		})
		return fut.Result()
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"replayed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRunInChildContextAsyncReplayFailed(t *testing.T) {
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "FAILED", &wireContextDetails{
			Error: &wireFullError{
				ErrorType:    "TestError",
				ErrorMessage: "test failure",
			},
		}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "mychild", func(childCtx Context) (string, error) {
			t.Fatal("fn should not execute on replay")
			return "", nil
		})
		_, err := fut.Result()
		if err == nil {
			t.Fatal("expected error from failed child")
		}
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			t.Fatalf("err is not ChildContextError: %v", err)
		}
		if childErr.Name != "mychild" {
			t.Errorf("ChildContextError.Name = %q, want mychild", childErr.Name)
		}
		return "handled", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"handled\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRunInChildContextAsyncFnError(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "failing", func(childCtx Context) (string, error) {
			return "", errors.New("child failed")
		})
		_, err := fut.Result()
		if err == nil {
			return "unexpected", nil
		}
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			return "", err
		}
		return "caught: " + childErr.Name, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"caught: failing\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	// Verify FAIL checkpoint was issued.
	updates := updateBatch(t, fake)
	found := false
	for _, u := range updates {
		if u.Action == OperationActionFail && u.Type == OperationTypeContext {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected a FAIL checkpoint for the child context")
	}
}

func TestRunInChildContextAsyncPanic(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "panicker", func(childCtx Context) (string, error) {
			panic("kaboom")
		})
		_, err := fut.Result()
		if err == nil {
			return "unexpected", nil
		}
		var childErr *ChildContextError
		if errors.As(err, &childErr) {
			return "panic caught", nil
		}
		return "", err
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"panic caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRunInChildContextAsyncSuspension(t *testing.T) {
	// Child contains a Wait which triggers suspension. The future should
	// settle with errSuspendExecution, and the invocation response is PENDING.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "waiter", func(childCtx Context) (string, error) {
			if err := Wait(childCtx, "w", 5*time.Second); err != nil {
				return "", err
			}
			return "done", nil
		})
		_, err := fut.Result()
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestGoIsRunInChildContextAsync(t *testing.T) {
	// Go is semantically identical to RunInChildContextAsync.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"input"`), func(ctx Context, event string) (string, error) {
		fut := Go(ctx, "my-task", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return event + "-via-go", nil
			})
		})
		return fut.Result()
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"input-via-go\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Concurrent Go tests ---

func TestMultipleGoFanOut(t *testing.T) {
	// Two concurrent Go calls. Both succeed. Results collected.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "a", func(childCtx Context) (string, error) {
			return Step(childCtx, "s1", func(StepContext) (string, error) {
				return "A", nil
			})
		})
		f2 := Go(ctx, "b", func(childCtx Context) (string, error) {
			return Step(childCtx, "s2", func(StepContext) (string, error) {
				return "B", nil
			})
		})
		a, err := f1.Result()
		if err != nil {
			return "", err
		}
		b, err := f2.Result()
		if err != nil {
			return "", err
		}
		return a + b, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"AB\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestMultipleGoOneSuspends(t *testing.T) {
	// Two Go calls; one suspends (Wait). The other should also
	// receive errSuspendExecution through future settlement.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "fast", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return "fast-done", nil
			})
		})
		f2 := Go(ctx, "slow", func(childCtx Context) (string, error) {
			if err := Wait(childCtx, "w", time.Minute); err != nil {
				return "", err
			}
			return "slow-done", nil
		})

		// Collect results. One of them will be errSuspendExecution.
		_, err1 := f1.Result()
		_, err2 := f2.Result()

		// At least one must be suspension.
		if !errors.Is(err1, errSuspendExecution) && !errors.Is(err2, errSuspendExecution) {
			t.Error("expected at least one future settled with errSuspendExecution")
		}
		// The combined error propagates suspension.
		if errors.Is(err1, errSuspendExecution) {
			return "", err1
		}
		return "", err2
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestGoGoroutineOwnership(t *testing.T) {
	// Operations inside a Go child must NOT fail with ErrWrongGoroutine.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "owned", func(childCtx Context) (string, error) {
			// This step runs on the child's goroutine, which owns
			// the child context. Must not fail with ownership error.
			return Step(childCtx, "inner", func(StepContext) (string, error) {
				return "ok", nil
			})
		})
		return fut.Result()
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestGoParentContextFromChildGoroutineFails(t *testing.T) {
	// Using the PARENT context from a child goroutine must fail fast with
	// ErrWrongGoroutine — not silently corrupt replay order.
	// This test exercises the goroutine ownership diagnostic, which is
	// only active under -tags=durablecheck.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "misuse", func(_ Context) (string, error) {
			// Attempt to use the parent ctx from the child goroutine.
			_, err := Step[string](ctx, "bad", func(StepContext) (string, error) {
				return "should not run", nil
			})
			return "", err
		})
		_, err := fut.Result()
		if err == nil {
			// Goroutine ownership check is disabled: the step succeeded
			// because check() is a no-op without the durablecheck tag.
			return "check-disabled", nil
		}
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			return "", fmt.Errorf("expected ChildContextError wrapping ErrWrongGoroutine, got: %v", err)
		}
		if !errors.Is(childErr.Err, ErrWrongGoroutine) {
			return "", fmt.Errorf("inner error = %v, want ErrWrongGoroutine", childErr.Err)
		}
		return "ownership-checked", nil
	})

	// Without -tags=durablecheck the goroutine check is a no-op, so the
	// step on the parent context succeeds and the handler returns early.
	if resp == `{"Status":"SUCCEEDED","Result":"\"check-disabled\""}` {
		t.Skip("goroutine ownership checks disabled without -tags=durablecheck")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"ownership-checked\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- StepAsync tests ---

func TestStepAsyncSuccess(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"data"`), func(ctx Context, event string) (string, error) {
		fut := StepAsync(ctx, "async-step", func(StepContext) (string, error) {
			return "async-" + event, nil
		})
		return fut.Result()
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"async-data\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestStepAsyncReplaySucceeded(t *testing.T) {
	fake := &fakeLambda{}
	payload := stepPayload(`"x"`,
		checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Result: `"replayed"`}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fut := StepAsync(ctx, "s", func(StepContext) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})
		return fut.Result()
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"replayed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestStepAsyncSuspension(t *testing.T) {
	// A step that fails and retries causes suspension. The future should
	// settle with errSuspendExecution.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := StepAsync(ctx, "retry-step", func(StepContext) (string, error) {
			return "", errors.New("transient")
		})
		_, err := fut.Result()
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- WaitAsync tests ---

func TestWaitAsyncSuspends(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := WaitAsync(ctx, "w", 10*time.Second)
		_, err := fut.Result()
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitAsyncReplaySucceeded(t *testing.T) {
	fake := &fakeLambda{}
	// Wait with SUCCEEDED status: returns immediately.
	payload := stepPayload(`"x"`, wireOperation{
		Id:     hashID("1"),
		Status: "SUCCEEDED",
	})
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fut := WaitAsync(ctx, "w", 10*time.Second)
		_, err := fut.Result()
		if err != nil {
			return "", err
		}
		return "continued", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"continued\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- InvokeAsync tests ---

func TestInvokeAsyncStartsAndSuspends(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"order"`), func(ctx Context, event string) (string, error) {
		fut := InvokeAsync[string](ctx, "charge", "arn:target:1", event)
		_, err := fut.Result()
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	if len(updates) != 1 {
		t.Fatalf("received %d updates, want 1", len(updates))
	}
	if updates[0].Type != OperationTypeChainedInvoke {
		t.Errorf("Type = %q, want CHAINED_INVOKE", updates[0].Type)
	}
}

func TestInvokeAsyncReplaySucceeded(t *testing.T) {
	fake := &fakeLambda{}
	payload := stepPayload(`"x"`, wireOperation{
		Id:                   hashID("1"),
		Status:               "SUCCEEDED",
		ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"result-from-invoke"`},
	})
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fut := InvokeAsync[string](ctx, "i", "target", "in")
		return fut.Result()
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"result-from-invoke\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Concurrent StepAsync + Go interaction ---

func TestStepAsyncAndGoInterleaved(t *testing.T) {
	// StepAsync and Go interleaved: deterministic ID allocation.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		stepFut := StepAsync(ctx, "s1", func(StepContext) (string, error) {
			return "step-result", nil
		})
		goFut := Go(ctx, "g1", func(childCtx Context) (string, error) {
			return Step(childCtx, "inner", func(StepContext) (string, error) {
				return "go-result", nil
			})
		})

		s, err := stepFut.Result()
		if err != nil {
			return "", err
		}
		g, err := goFut.Result()
		if err != nil {
			return "", err
		}
		return s + "+" + g, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"step-result+go-result\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	// Verify IDs: step should be "1", Go child should be "2".
	updates := updateBatch(t, fake)
	var stepStartID, ctxStartID string
	for _, u := range updates {
		// Root-level step: Type STEP, no ParentId.
		if u.Type == OperationTypeStep && u.Action == OperationActionStart && u.ParentId == nil {
			stepStartID = aws.ToString(u.Id)
		}
		if u.Type == OperationTypeContext && u.Action == OperationActionStart {
			ctxStartID = aws.ToString(u.Id)
		}
	}
	if stepStartID != hashID("1") {
		t.Errorf("step ID = %q, want hash of '1' (%q)", stepStartID, hashID("1"))
	}
	if ctxStartID != hashID("2") {
		t.Errorf("context ID = %q, want hash of '2' (%q)", ctxStartID, hashID("2"))
	}
}

// --- childReplayMode tests ---

func TestChildReplayModeTable(t *testing.T) {
	tests := []struct {
		name     string
		op       *operation
		hasChild bool // whether state has id+"-1"
		want     executionMode
	}{
		{
			name: "nil op no children",
			op:   nil,
			want: modeExecution,
		},
		{
			name:     "nil op has children",
			op:       nil,
			hasChild: true,
			want:     modeReplay,
		},
		{
			name: "STARTED no children",
			op:   &operation{status: statusStarted},
			want: modeExecution,
		},
		{
			name:     "STARTED has children",
			op:       &operation{status: statusStarted},
			hasChild: true,
			want:     modeReplay,
		},
		{
			name: "SUCCEEDED",
			op:   &operation{status: statusSucceeded},
			want: modeReplaySucceededContext,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ops := make(map[string]*operation)
			if tc.hasChild {
				ops[hashID("1-1")] = &operation{status: statusStarted}
			}
			state := &executionState{operations: ops}
			ec := &execContext{
				Context: context.Background(),
				state:   state,
			}
			got := childReplayMode(ec, "1", tc.op)
			if got != tc.want {
				t.Errorf("childReplayMode() = %d, want %d", got, tc.want)
			}
		})
	}
}

// --- Suspension wins over user code that swallows errors ---

func TestSuspensionWinsOverSwallowedError(t *testing.T) {
	// If user code catches the suspension error and returns success, the
	// invocation response is still PENDING because the handler's select
	// observes the fired suspend signal.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		// This Wait fires suspension.
		_ = Wait(ctx, "w", time.Second)
		// User code swallows the error and "succeeds":
		return "user thinks success", nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- ReplayChildren mode tests ---

func TestRunInChildContextReplayChildrenTrigger(t *testing.T) {
	// When the child result exceeds checkpointSizeLimitBytes, the SUCCEED
	// checkpoint carries ContextOptions.ReplayChildren=true and no Payload.
	fake := &fakeLambda{}
	largeResult := strings.Repeat("x", checkpointSizeLimitBytes+1)
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return largeResult, nil
			})
		})
	})

	// Should succeed (the large result is returned inline on first
	// execution; ReplayChildren only matters on subsequent replays).
	if resp == `{"Status":"PENDING"}` {
		t.Skip("suspended unexpectedly")
	}

	updates := updateBatch(t, fake)
	var ctxSucceed *OperationUpdate
	for i := range updates {
		if updates[i].Type == OperationTypeContext && updates[i].Action == OperationActionSucceed {
			ctxSucceed = &updates[i]
		}
	}
	if ctxSucceed == nil {
		t.Fatal("expected a SUCCEED checkpoint for the context operation")
	}
	if ctxSucceed.Payload != nil {
		t.Error("large-payload SUCCEED should have nil Payload")
	}
	if ctxSucceed.ContextOptions == nil || !aws.ToBool(ctxSucceed.ContextOptions.ReplayChildren) {
		t.Error("expected ContextOptions.ReplayChildren = true")
	}
}

func TestRunInChildContextReplayChildrenSmallPayload(t *testing.T) {
	// When the child result is small, Payload is set and ReplayChildren is
	// not used.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "small", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return "tiny", nil
			})
		})
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"tiny\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	for _, u := range updates {
		if u.Type == OperationTypeContext && u.Action == OperationActionSucceed {
			if u.Payload == nil {
				t.Error("small result should have Payload set")
			}
			if u.ContextOptions != nil {
				t.Error("small result should not set ContextOptions")
			}
		}
	}
}

func TestRunInChildContextReplayChildrenReExecution(t *testing.T) {
	// On replay of a SUCCEEDED child with replayChildren=true, the child
	// body is re-executed to reconstruct the result.
	fake := &fakeLambda{}
	fnCalled := false
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		// The child's inner step must also be in the checkpoint log
		// so the child's replay finds it.
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"reconstructed"`},
		},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "big", func(childCtx Context) (string, error) {
			fnCalled = true
			return Step(childCtx, "s", func(StepContext) (string, error) {
				t.Fatal("step body should not execute on replay")
				return "", nil
			})
		})
	})

	if !fnCalled {
		t.Error("child body should be re-executed in ReplayChildren mode")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"reconstructed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRunInChildContextAsyncReplayChildrenReExecution(t *testing.T) {
	// Async variant: replay of a SUCCEEDED child with replayChildren=true
	// re-executes the child body.
	fake := &fakeLambda{}
	fnCalled := false
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"async-reconstructed"`},
		},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "big-async", func(childCtx Context) (string, error) {
			fnCalled = true
			return Step(childCtx, "s", func(StepContext) (string, error) {
				t.Fatal("step body should not execute on replay")
				return "", nil
			})
		})
		return fut.Result()
	})

	if !fnCalled {
		t.Error("child body should be re-executed in async ReplayChildren mode")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"async-reconstructed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRunInChildContextAsyncReplayChildrenLargePayload(t *testing.T) {
	// Async variant: large payload triggers ContextOptions.ReplayChildren.
	fake := &fakeLambda{}
	largeResult := strings.Repeat("x", checkpointSizeLimitBytes+1)
	invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "big-async", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return largeResult, nil
			})
		})
		return fut.Result()
	})

	updates := updateBatch(t, fake)
	var ctxSucceed *OperationUpdate
	for i := range updates {
		if updates[i].Type == OperationTypeContext && updates[i].Action == OperationActionSucceed {
			ctxSucceed = &updates[i]
		}
	}
	if ctxSucceed == nil {
		t.Fatal("expected a SUCCEED checkpoint for the async context")
	}
	if ctxSucceed.Payload != nil {
		t.Error("large-payload async SUCCEED should have nil Payload")
	}
	if ctxSucceed.ContextOptions == nil || !aws.ToBool(ctxSucceed.ContextOptions.ReplayChildren) {
		t.Error("expected ContextOptions.ReplayChildren = true for async")
	}
}

func TestRunInChildContextReplayChildrenNoResult(t *testing.T) {
	// ReplayChildren=true but Result field empty (expected behavior):
	// the child body reconstructs the value from its inner operations.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{ReplayChildren: true, Result: ""}),
		wireOperation{
			Id:          hashID("1-1"),
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"from-inner"`},
		},
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return RunInChildContext(ctx, "replay", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				t.Fatal("step should replay from checkpoint")
				return "", nil
			})
		})
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"from-inner\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Go option forwarding ---

// succeedPayload returns the Payload of the SUCCEED update recorded for the
// context operation with the given positional ID.
func succeedPayload(t *testing.T, fake *fakeLambda, positionalID string) string {
	t.Helper()
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeContext && u.Action == OperationActionSucceed &&
			aws.ToString(u.Id) == hashID(positionalID) {
			return aws.ToString(u.Payload)
		}
	}
	t.Fatalf("no SUCCEED update for context operation %q", positionalID)
	return ""
}

func TestGoForwardsChildSerdes(t *testing.T) {
	// Go is shorthand for RunInChildContextAsync, so WithChildSerdes
	// passed to Go must serialize the child result. upperSerdes uppercases
	// on Marshal and decodes as standard JSON, so the transformed value is
	// visible both in the checkpoint and in the future's result.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "child", func(Context) (string, error) {
			return "hello", nil
		}, WithChildSerdes(upperSerdes{}))
		return fut.Result()
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"HELLO\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if got := succeedPayload(t, fake, "1"); got != `"HELLO"` {
		t.Errorf("child SUCCEED Payload = %q, want %q", got, `"HELLO"`)
	}
}

func TestGoChildSerdesReplay(t *testing.T) {
	// On replay of a SUCCEEDED child, Go decodes the stored result with
	// the serdes it was given, exactly as RunInChildContextAsync does. A
	// serdes that cannot decode the stored value surfaces as a SerdesError
	// through the future.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: `"stored"`}))
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "child", func(Context) (string, error) {
			t.Error("child body must not re-execute on replay")
			return "", nil
		}, WithChildSerdes(failingSerdes{failUnmarshal: true, cause: cause}))
		_, got = fut.Result()
		return "", nil
	})
	assertSerdesError(t, got, "child", "unmarshal", cause)
}
