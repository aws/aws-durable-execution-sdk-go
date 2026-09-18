package durable

import (
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
)

// Every *Future[O] satisfies Awaitable without the caller naming O.
var (
	_ Awaitable = (*Future[int])(nil)
	_ Awaitable = (*Future[string])(nil)
	_ Awaitable = (*Future[Void])(nil)
	_ Awaitable = (*Future[[]byte])(nil)
	_ Awaitable = (*Future[struct{ A, B int }])(nil)
)

func TestJoinHeterogeneousSuccess(t *testing.T) {
	// Join awaits futures of different result types; values are read
	// afterwards with Result.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fa := Go(ctx, "count", func(childCtx Context) (int, error) {
			return Step(childCtx, "s1", func(StepContext) (int, error) {
				return 7, nil
			})
		})
		fb := Go(ctx, "label", func(childCtx Context) (string, error) {
			return Step(childCtx, "s2", func(StepContext) (string, error) {
				return "seven", nil
			})
		})
		fc := StepAsync(ctx, "flag", func(StepContext) (bool, error) {
			return true, nil
		})

		if err := Join(ctx, "settle", []Awaitable{fa, fb, fc}); err != nil {
			return "", err
		}
		n, err := fa.Result()
		if err != nil {
			return "", err
		}
		s, err := fb.Result()
		if err != nil {
			return "", err
		}
		flag, err := fc.Result()
		if err != nil {
			return "", err
		}
		return s + "=" + strconv.Itoa(n) + " " + strconv.FormatBool(flag), nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"seven=7 true\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestJoinReturnsFirstErrorInArgumentOrder(t *testing.T) {
	// Two futures fail. Join returns the error of the earlier one in
	// argument order, wrapped as a *ChildContextError named after the
	// combinator, regardless of which settled first (both are
	// pre-settled here).
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newSettledFuture(1, nil)
		f2 := newFailedFuture[string](errors.New("second-failed"))
		f3 := newFailedFuture[bool](errors.New("third-failed"))

		err := Join(ctx, "settle", []Awaitable{f1, f2, f3})
		if err == nil {
			return "unexpected-success", nil
		}
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			return "other-err", nil
		}
		return childErr.Name + ": " + childErr.Message, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"settle: second-failed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// TestJoinAwaitsAllAfterError verifies that Join does not fail fast: an
// error observed before any suspension does not stop Join from awaiting
// the remaining futures. The later future records the await from its
// preResult hook and settles itself, so a fail-fast regression surfaces as
// a missed await rather than a hang. The later future's own failure is not
// the one returned, because the first error in argument order wins.
func TestJoinAwaitsAllAfterError(t *testing.T) {
	fake := &fakeLambda{}
	var laterAwaited atomic.Bool

	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFailedFuture[string](errors.New("first-failure"))
		f2 := newFuture[int]()
		f2.preResult = func() {
			laterAwaited.Store(true)
			f2.settle(0, errors.New("later-failure"))
		}

		err := Join(ctx, "settle", []Awaitable{f1, f2})
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			return "other-err", nil
		}
		return childErr.Message, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"first-failure\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if !laterAwaited.Load() {
		t.Error("Join returned without awaiting the future after the first error")
	}
}

// TestJoinDrainsSuspendedThenFailing verifies the drain contract shared
// with All. f1 suspends on a pending callback. f2 is released only when
// Join attempts to await it, then runs a step and fails. The step can only
// be checkpointed if Join drained past f1's suspension to f2, and the
// suspension is propagated in preference to f2's failure.
func TestJoinDrainsSuspendedThenFailing(t *testing.T) {
	fake := &fakeLambda{}
	gate := make(chan struct{})
	errCh := make(chan error, 1)

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb1")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})
		f2 := Go(ctx, "failer", func(childCtx Context) (int, error) {
			<-gate
			if _, err := Step(childCtx, "trailing-step", func(StepContext) (int, error) {
				return 1, nil
			}); err != nil {
				return 0, err
			}
			return 0, errors.New("failer-failed")
		})
		f2.preResult = func() { close(gate) }

		err := Join(ctx, "settle", []Awaitable{f1, f2})
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("Join error = %v, want errSuspendExecution", err)
	}
	assertStepCheckpointed(t, fake, "trailing-step")
}

// TestJoinFailingThenSuspendedPropagatesSuspension verifies the other
// order: a failure observed first does not become Join's outcome when a
// later future suspends, because the suspended branch completes only on a
// later invocation and the failure is returned on replay then.
func TestJoinFailingThenSuspendedPropagatesSuspension(t *testing.T) {
	fake := &fakeLambda{}
	errCh := make(chan error, 1)

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFailedFuture[int](errors.New("first-failure"))
		f2 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb1")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})

		err := Join(ctx, "settle", []Awaitable{f1, f2})
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("Join error = %v, want errSuspendExecution", err)
	}
}

func TestJoinEmpty(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		if err := Join(ctx, "empty", nil); err != nil {
			return "", err
		}
		if err := Join(ctx, "empty-literal", []Awaitable{}); err != nil {
			return "", err
		}
		return "ok", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestJoinCheckpointsOutcome(t *testing.T) {
	// Join records one child-context operation whose SUCCEED payload is
	// the serialized Void result. The futures are pre-settled and claim no
	// operation IDs, so Join is positional ID 1.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		err := Join(ctx, "settle", []Awaitable{
			newSettledFuture(1, nil),
			newSettledFuture("two", nil),
		})
		if err != nil {
			return "", err
		}
		return "ok", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if got, want := succeedPayload(t, fake, "1"), `{}`; got != want {
		t.Errorf("Join SUCCEED Payload = %q, want %q", got, want)
	}
}

func TestJoinReplaySuccess(t *testing.T) {
	// With the Join operation checkpointed as SUCCEEDED, Join returns nil
	// without re-awaiting, and the futures replay their stored values.
	// Go x2 = IDs 1,2; Join = ID 3.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: `7`}),
		checkpointedChild("2", "SUCCEEDED", &wireContextDetails{Result: `"seven"`}),
		checkpointedChild("3", "SUCCEEDED", &wireContextDetails{Result: `{}`}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		fa := Go(ctx, "count", func(Context) (int, error) {
			t.Fatal("should not execute on replay")
			return 0, nil
		})
		fb := Go(ctx, "label", func(Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})
		if err := Join(ctx, "settle", []Awaitable{fa, fb}); err != nil {
			return "", err
		}
		n, _ := fa.Result()
		s, _ := fb.Result()
		return s + "=" + strconv.Itoa(n), nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"seven=7\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if len(fake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0", len(fake.gotUpdateBatches))
	}
}

func TestJoinReplayFailure(t *testing.T) {
	// With the Join operation checkpointed as FAILED, the stored failure is
	// returned as a *ChildContextError without awaiting any future. The
	// unsettled future would hang if Join awaited it.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "FAILED", &wireContextDetails{
			Error: &wireFullError{ErrorType: "Error", ErrorMessage: "second-failed"},
		}),
	)
	var awaited atomic.Bool
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		f := newFuture[int]()
		f.preResult = func() {
			awaited.Store(true)
			f.settle(0, nil)
		}
		err := Join(ctx, "settle", []Awaitable{f})
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			return "other-err", nil
		}
		return childErr.Name + ": " + childErr.Message, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"settle: second-failed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if awaited.Load() {
		t.Error("Join awaited a future on replay of a checkpointed outcome")
	}
	if len(fake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0", len(fake.gotUpdateBatches))
	}
}

func TestJoinForwardsChildOptions(t *testing.T) {
	// opts reach the child-context operation: a WithChildErrorMapper
	// mapper sees the first error and its result is what Join returns.
	fake := &fakeLambda{}
	sentinel := errors.New("mapped")
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		err := Join(ctx, "settle", []Awaitable{
			newFailedFuture[int](errors.New("first-failure")),
		}, WithChildErrorMapper(func(ce *ChildContextError) error {
			if ce.Message != "first-failure" {
				return nil
			}
			return sentinel
		}))
		if errors.Is(err, sentinel) {
			return "mapped", nil
		}
		return "unmapped", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"mapped\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- awaitBarrier, the drain logic shared by All and Join ---

func TestAwaitBarrierOrdering(t *testing.T) {
	suspended := newFailedFuture[int](errSuspendExecution)
	failed := func(msg string) *Future[int] { return newFailedFuture[int](errors.New(msg)) }
	ok := newSettledFuture(1, nil)

	tests := []struct {
		name     string
		fs       []Awaitable
		failFast bool
		want     string // "" for nil, "suspend" for the sentinel
	}{
		{"empty", nil, false, ""},
		{"all ok", []Awaitable{ok, ok}, false, ""},
		{"first error wins", []Awaitable{ok, failed("a"), failed("b")}, false, "a"},
		{"first error wins fail-fast", []Awaitable{ok, failed("a"), failed("b")}, true, "a"},
		{"suspend beats earlier error", []Awaitable{failed("a"), suspended}, false, "suspend"},
		{"suspend beats later error", []Awaitable{suspended, failed("a")}, false, "suspend"},
		{"suspend beats later error fail-fast", []Awaitable{suspended, failed("a")}, true, "suspend"},
		{"suspend beats later ok", []Awaitable{suspended, ok}, true, "suspend"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := awaitBarrier(tc.fs, tc.failFast)
			switch tc.want {
			case "":
				if err != nil {
					t.Fatalf("awaitBarrier = %v, want nil", err)
				}
			case "suspend":
				if !errors.Is(err, errSuspendExecution) {
					t.Fatalf("awaitBarrier = %v, want errSuspendExecution", err)
				}
			default:
				if err == nil || err.Error() != tc.want {
					t.Fatalf("awaitBarrier = %v, want %q", err, tc.want)
				}
			}
		})
	}
}

// TestAwaitBarrierFailFastStopsAwaiting verifies the one behavioural
// difference between All and Join: with failFast the barrier returns on
// the first error before any suspension and does not await later futures;
// without failFast it awaits them.
func TestAwaitBarrierFailFastStopsAwaiting(t *testing.T) {
	for _, failFast := range []bool{true, false} {
		var laterAwaited atomic.Bool
		later := newFuture[int]()
		later.preResult = func() {
			laterAwaited.Store(true)
			later.settle(0, nil)
		}
		err := awaitBarrier([]Awaitable{newFailedFuture[int](errors.New("first")), later}, failFast)
		if err == nil || err.Error() != "first" {
			t.Fatalf("failFast=%v: awaitBarrier = %v, want first", failFast, err)
		}
		if laterAwaited.Load() == failFast {
			t.Errorf("failFast=%v: later future awaited = %v", failFast, laterAwaited.Load())
		}
	}
}
