package durable

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// --- All tests ---

func TestAllSuccess(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "a", func(childCtx Context) (string, error) {
			return Step(childCtx, "s1", func(StepContext) (string, error) {
				return "one", nil
			})
		})
		f2 := Go(ctx, "b", func(childCtx Context) (string, error) {
			return Step(childCtx, "s2", func(StepContext) (string, error) {
				return "two", nil
			})
		})
		f3 := Go(ctx, "c", func(childCtx Context) (string, error) {
			return Step(childCtx, "s3", func(StepContext) (string, error) {
				return "three", nil
			})
		})

		results, err := All(ctx, "all-op", []*Future[string]{f1, f2, f3})
		if err != nil {
			return "", err
		}
		// Results should be in input order.
		out := ""
		for _, r := range results {
			out += r + ","
		}
		return out, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"one,two,three,\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllFailFast(t *testing.T) {
	// All fails when any future fails: the ChildContextError propagates.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newSettledFuture("good", nil)
		f2 := newFailedFuture[string](errors.New("step failed"))

		_, err := All(ctx, "all-op", []*Future[string]{f1, f2})
		if err == nil {
			return "unexpected-success", nil
		}
		var childErr *ChildContextError
		if errors.As(err, &childErr) {
			return "all-failed: " + childErr.Name, nil
		}
		return "other-err", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"all-failed: all-op\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllEmpty(t *testing.T) {
	// All([]) returns empty slice immediately.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		results, err := All(ctx, "empty-all", []*Future[string]{})
		if err != nil {
			return "", err
		}
		if len(results) != 0 {
			return "non-empty", nil
		}
		return "empty", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"empty\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllSuspension(t *testing.T) {
	// When a future suspends, All propagates suspension.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "fast", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return "ok", nil
			})
		})
		f2 := Go(ctx, "slow", func(childCtx Context) (string, error) {
			if err := Wait(childCtx, "w", time.Minute); err != nil {
				return "", err
			}
			return "done", nil
		})

		_, err := All(ctx, "all-suspend", []*Future[string]{f1, f2})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllReplaySuccess(t *testing.T) {
	// When the All child context is already checkpointed as SUCCEEDED,
	// the stored result is returned without re-awaiting futures.
	fake := &fakeLambda{}
	// The All combinator uses the 4th operation ID (after Go x3 = IDs 1,2,3 → All = ID 4).
	storedResult := `["a","b","c"]`
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: `"one"`}),
		checkpointedChild("2", "SUCCEEDED", &wireContextDetails{Result: `"two"`}),
		checkpointedChild("3", "SUCCEEDED", &wireContextDetails{Result: `"three"`}),
		checkpointedChild("4", "SUCCEEDED", &wireContextDetails{Result: storedResult}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "a", func(childCtx Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})
		f2 := Go(ctx, "b", func(childCtx Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})
		f3 := Go(ctx, "c", func(childCtx Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})

		results, err := All(ctx, "all-op", []*Future[string]{f1, f2, f3})
		if err != nil {
			return "", err
		}
		out := ""
		for _, r := range results {
			out += r + ","
		}
		return out, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"a,b,c,\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllCheckpoints(t *testing.T) {
	// Verify that All issues a child-context checkpoint.
	fake := &fakeLambda{}
	invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "a", func(childCtx Context) (string, error) {
			return Step(childCtx, "s", func(StepContext) (string, error) {
				return "val", nil
			})
		})
		_, err := All(ctx, "all-cp", []*Future[string]{f1})
		if err != nil {
			return "", err
		}
		return "ok", nil
	})

	// All creates a RunInChildContext: START + SUCCEED.
	updates := updateBatch(t, fake)
	var contextUpdates []types.OperationUpdate
	for _, u := range updates {
		if u.Type == types.OperationTypeContext {
			contextUpdates = append(contextUpdates, u)
		}
	}
	// We expect at least: Go("a") START/SUCCEED + All("all-cp") START/SUCCEED.
	if len(contextUpdates) < 4 {
		t.Errorf("expected at least 4 context updates, got %d", len(contextUpdates))
	}
}

// --- AllSettled tests ---

func TestAllSettledMixed(t *testing.T) {
	// AllSettled collects all outcomes, both success and failure.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newSettledFuture("good", nil)
		f2 := newFailedFuture[string](errors.New("step failed"))

		settled, err := AllSettled(ctx, "settled-op", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		// Build a result string describing the outcomes.
		result := ""
		for i, s := range settled {
			if s.Err != nil {
				result += "err"
			} else {
				result += s.Value
			}
			if i < len(settled)-1 {
				result += ","
			}
		}
		return result, nil
	})

	// f1 succeeds (but gives ChildContextError from Go), f2 fails.
	// The settled values come from the futures: Go wraps fn errors as ChildContextError.
	// So f1.Result() = ("good", nil), f2.Result() = ("", *ChildContextError)
	if want := `{"Status":"SUCCEEDED","Result":"\"good,err\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllSettledAllSuccess(t *testing.T) {
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

		settled, err := AllSettled(ctx, "all-settled", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		out := ""
		for _, s := range settled {
			if s.Err != nil {
				out += "ERR,"
			} else {
				out += s.Value + ","
			}
		}
		return out, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"A,B,\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllSettledEmpty(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		settled, err := AllSettled(ctx, "empty", []*Future[string]{})
		if err != nil {
			return "", err
		}
		if len(settled) != 0 {
			return "non-empty", nil
		}
		return "empty", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"empty\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllSettledSuspension(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			if err := Wait(childCtx, "w", time.Minute); err != nil {
				return "", err
			}
			return "done", nil
		})

		_, err := AllSettled(ctx, "settled-suspend", []*Future[string]{f1})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAllSettledReplay(t *testing.T) {
	// AllSettled result is replayed from checkpoint.
	fake := &fakeLambda{}
	// Settled[string] serializes via MarshalJSON as {status,value,error}.
	settledResult := `[{"status":"fulfilled","value":"ok"},{"status":"rejected","error":"bad"}]`
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: settledResult}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		// No futures needed — the combinator is fully replayed from checkpoint.
		// We pass an empty slice because the child context is terminal and
		// won't re-execute. But the stub actually needs the right ID claim
		// count. Since we checkpoint only 1 op (ID "1" = the AllSettled child
		// itself), and AllSettled is the first claim, this works directly.
		settled, err := AllSettled(ctx, "settled-replay", []*Future[string]{})
		if err != nil {
			return "", err
		}
		out := ""
		for _, s := range settled {
			if s.Err != nil {
				out += "err,"
			} else {
				out += s.Value + ","
			}
		}
		return out, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"ok,err,\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Any tests ---

func TestAnyFirstSuccess(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFailedFuture[string](errors.New("fails"))
		f2 := newSettledFuture("winner", nil)

		result, err := Any(ctx, "any-op", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		return result, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"winner\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAnyAllFail(t *testing.T) {
	// When all futures fail, Any returns CombinatorError wrapped in ChildContextError.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFailedFuture[string](errors.New("error-1"))
		f2 := newFailedFuture[string](errors.New("error-2"))

		_, err := Any(ctx, "any-all-fail", []*Future[string]{f1, f2})
		if err == nil {
			return "unexpected-success", nil
		}
		var combErr *CombinatorError
		if errors.As(err, &combErr) {
			return combErr.Name, nil
		}
		return "other-err", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"any-all-fail\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAnyEmpty(t *testing.T) {
	// Any([]) fails immediately with CombinatorError.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := Any(ctx, "any-empty", []*Future[string]{})
		if err == nil {
			return "unexpected-success", nil
		}
		var combErr *CombinatorError
		if errors.As(err, &combErr) {
			return "empty-combinator-error", nil
		}
		return "other-err", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"empty-combinator-error\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAnySuspension(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			if err := Wait(childCtx, "w", time.Minute); err != nil {
				return "", err
			}
			return "done", nil
		})

		_, err := Any(ctx, "any-suspend", []*Future[string]{f1})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAnyReplayDeterminism(t *testing.T) {
	// When Any's child context is replayed with a checkpointed result,
	// the same winner is returned deterministically.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: `"loser"`}),
		checkpointedChild("2", "SUCCEEDED", &wireContextDetails{Result: `"winner"`}),
		checkpointedChild("3", "SUCCEEDED", &wireContextDetails{Result: `"winner"`}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "a", func(childCtx Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})
		f2 := Go(ctx, "b", func(childCtx Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})

		result, err := Any(ctx, "any-replay", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		return result, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"winner\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Race tests ---

func TestRaceFirstSettlesSuccess(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "fast", func(childCtx Context) (string, error) {
			return Step(childCtx, "s1", func(StepContext) (string, error) {
				return "first", nil
			})
		})
		f2 := Go(ctx, "slow", func(childCtx Context) (string, error) {
			return Step(childCtx, "s2", func(StepContext) (string, error) {
				return "second", nil
			})
		})

		result, err := Race(ctx, "race-op", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		return result, nil
	})

	// Both futures settle quickly; Race returns whichever settles first.
	// We can't predict which is first in practice, but one of them wins.
	var r invocationResponse
	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if r.Status != "SUCCEEDED" {
		t.Errorf("status = %s, want SUCCEEDED", r.Status)
	}
	if r.Result == nil || (*r.Result != `"first"` && *r.Result != `"second"`) {
		t.Errorf("result = %v, want first or second", r.Result)
	}
}

func TestRaceFirstSettlesError(t *testing.T) {
	// Race propagates an error when the sole future is pre-settled with a
	// failure. Using a single pre-settled future makes the test
	// deterministic: there is no competing goroutine that could win.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fErr := newFailedFuture[string](errors.New("immediate-fail"))
		_, err := Race(ctx, "race-err", []*Future[string]{fErr})
		if err == nil {
			return "unexpected-success", nil
		}
		var childErr *ChildContextError
		if errors.As(err, &childErr) {
			return "race-errored: " + childErr.Name, nil
		}
		return "other", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"race-errored: race-err\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRaceEmpty(t *testing.T) {
	// Race([]) suspends: no future will ever settle (matches Promise.race([])).
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := Race(ctx, "race-empty", []*Future[string]{})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRaceSuspension(t *testing.T) {
	// When all futures suspend, Race suspends.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "w1", func(childCtx Context) (string, error) {
			if err := Wait(childCtx, "w", time.Minute); err != nil {
				return "", err
			}
			return "done", nil
		})

		_, err := Race(ctx, "race-suspend", []*Future[string]{f1})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRaceReplayDeterminism(t *testing.T) {
	// When Race's child context is replayed with a checkpointed winner,
	// the same winner is returned.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: `"a"`}),
		checkpointedChild("2", "SUCCEEDED", &wireContextDetails{Result: `"b"`}),
		checkpointedChild("3", "SUCCEEDED", &wireContextDetails{Result: `"a"`}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "a", func(childCtx Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})
		f2 := Go(ctx, "b", func(childCtx Context) (string, error) {
			t.Fatal("should not execute on replay")
			return "", nil
		})

		result, err := Race(ctx, "race-replay", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		return result, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"a\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- CombinatorError tests ---

func TestCombinatorErrorMessage(t *testing.T) {
	err := &CombinatorError{
		Name:   "test-combinator",
		Errors: []error{errors.New("a"), errors.New("b")},
	}
	want := `durable: combinator "test-combinator": all futures failed (2 errors)`
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestCombinatorErrorUnwrap(t *testing.T) {
	inner1 := errors.New("first")
	inner2 := errors.New("second")
	err := &CombinatorError{
		Name:   "test",
		Errors: []error{inner1, inner2},
	}
	if !errors.Is(err, inner1) {
		t.Error("errors.Is(err, inner1) = false, want true")
	}
	if !errors.Is(err, inner2) {
		t.Error("errors.Is(err, inner2) = false, want true")
	}
}

func TestCombinatorErrorNilErrors(t *testing.T) {
	err := &CombinatorError{Name: "empty-any", Errors: nil}
	want := `durable: combinator "empty-any": all futures failed (0 errors)`
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

// --- Pre-settled future combinator tests ---

func TestAllPreSettledFutures(t *testing.T) {
	// Combinators work with pre-settled futures (no goroutines needed).
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newSettledFuture("pre-a", nil)
		f2 := newSettledFuture("pre-b", nil)

		results, err := All(ctx, "pre-all", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		return results[0] + "," + results[1], nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"pre-a,pre-b\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestAnyPreSettledFirstSuccess(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFailedFuture[string](errors.New("nope"))
		f2 := newSettledFuture("yes", nil)

		result, err := Any(ctx, "pre-any", []*Future[string]{f1, f2})
		if err != nil {
			return "", err
		}
		return result, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"yes\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestRacePreSettledError(t *testing.T) {
	// Race with a pre-settled error future returns that error.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFailedFuture[string](errors.New("fast-err"))

		_, err := Race(ctx, "pre-race", []*Future[string]{f1})
		if err == nil {
			return "unexpected", nil
		}
		var childErr *ChildContextError
		if errors.As(err, &childErr) {
			return "caught", nil
		}
		return "other", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Suspension drain tests ---

// observeRecorder captures every outcome passed to combinatorObserve. It
// lets tests both release sibling futures once a suspension has been
// observed and prove, at the moment the combinator returns, exactly how
// many outcomes the receive loop had observed.
type observeRecorder struct {
	mu       sync.Mutex
	outcomes []error
}

// snapshot returns a copy of the outcomes observed so far.
func (o *observeRecorder) snapshot() []error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]error(nil), o.outcomes...)
}

// setCombinatorObserve installs an observation hook for the Any and Race
// receive loops and restores the previous hook on test cleanup. The
// returned channel is closed the first time a suspension outcome is
// observed, letting tests release sibling futures only after the
// combinator has seen the suspension. The returned recorder accumulates
// every observed outcome so tests can assert the combinator drained all
// futures before returning.
func setCombinatorObserve(t *testing.T) (<-chan struct{}, *observeRecorder) {
	t.Helper()
	prev := combinatorObserve
	suspendObserved := make(chan struct{})
	rec := &observeRecorder{}
	var once sync.Once
	combinatorObserve = func(err error) {
		rec.mu.Lock()
		rec.outcomes = append(rec.outcomes, err)
		rec.mu.Unlock()
		if err != nil && errors.Is(err, errSuspendExecution) {
			once.Do(func() { close(suspendObserved) })
		}
	}
	t.Cleanup(func() { combinatorObserve = prev })
	return suspendObserved, rec
}

// assertDrainedOutcomes asserts that outcomes (captured at the moment the
// combinator returned) contains exactly total observations: one suspension,
// one success, and one non-suspension error. This proves the receive loop
// observed every future's outcome before returning.
func assertDrainedOutcomes(t *testing.T, outcomes []error, total int) {
	t.Helper()
	if len(outcomes) != total {
		t.Fatalf("observed %d outcomes at return, want %d: %v",
			len(outcomes), total, outcomes)
	}
	var suspends, successes, failures int
	for _, err := range outcomes {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, errSuspendExecution):
			suspends++
		default:
			failures++
		}
	}
	if suspends != 1 || successes != 1 || failures != 1 {
		t.Errorf("observed outcomes = %d suspend, %d success, %d failure; want 1 each: %v",
			suspends, successes, failures, outcomes)
	}
}

// assertStepCheckpointed asserts that the named step recorded both a START
// and a SUCCEED action in the fake's update batches.
func assertStepCheckpointed(t *testing.T, fake *fakeLambda, name string) {
	t.Helper()
	var start, succeed bool
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.Name) == name {
			switch u.Action {
			case types.OperationActionStart:
				start = true
			case types.OperationActionSucceed:
				succeed = true
			}
		}
	}
	if !start || !succeed {
		t.Errorf("expected %s START+SUCCEED, got start=%v succeed=%v",
			name, start, succeed)
	}
}

// TestAllDrainsSuspendedSiblings verifies that All awaits all futures and
// propagates the suspension, ensuring concurrent siblings reach their
// blocking points and checkpoint progress. The interleaving is forced:
// child 2 is released specifically when All attempts f2.Result, using f2's
// preResult hook as the synchronization gate, so child 2's submitter step
// cannot run unless All drains past f1's suspension.
func TestAllDrainsSuspendedSiblings(t *testing.T) {
	fake := &fakeLambda{}
	gate := make(chan struct{})
	errCh := make(chan error, 1)

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		// Child 1: create a callback and await it, suspending the branch.
		f1 := Go(ctx, "child1", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb1")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})

		// Child 2: wait for the gate, run a submitter step, then create
		// a callback and await it, suspending the branch.
		f2 := Go(ctx, "child2", func(childCtx Context) (string, error) {
			<-gate
			_, err := Step(childCtx, "submitter", func(_ StepContext) (string, error) {
				return "submitted", nil
			})
			if err != nil {
				return "", err
			}
			cb, err := CreateCallback[string](childCtx, "cb2")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})

		// The gate opens when All attempts f2.Result, releasing child 2.
		f2.preResult = func() { close(gate) }

		_, err := All(ctx, "all-drain", []*Future[string]{f1, f2})
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("All error = %v, want errSuspendExecution", err)
	}
	assertStepCheckpointed(t, fake, "submitter")
}

// TestAllSuspendThenTerminalPropagatesSuspension verifies that with three
// futures, All propagates the suspension even when terminal outcomes (a
// success and a real error) follow it. All awaits futures in input order,
// so f2's success and f3's error are observed strictly after f1's
// suspension.
func TestAllSuspendThenTerminalPropagatesSuspension(t *testing.T) {
	fake := &fakeLambda{}
	errCh := make(chan error, 1)

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb1")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})
		f2 := newSettledFuture("ok", nil)
		f3 := newFailedFuture[string](errors.New("real-failure"))

		_, err := All(ctx, "all-suspend-first", []*Future[string]{f1, f2, f3})
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("All error = %v, want errSuspendExecution", err)
	}
}

// TestAllDrainsAfterRealError verifies that when a real error follows a
// suspension in the future list, All still drains the remaining futures so
// their branches reach blocking points, and the suspension takes precedence
// over the error. Child 3 is released specifically when All attempts
// f3.Result, so its trailing step can only run if the drain continues past
// the real error.
func TestAllDrainsAfterRealError(t *testing.T) {
	fake := &fakeLambda{}
	gate := make(chan struct{})
	errCh := make(chan error, 1)

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		// f1: creates a pending callback and awaits it, suspending.
		f1 := Go(ctx, "child1", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb1")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})

		// f2: pre-settled with a real error.
		f2 := newFailedFuture[string](errors.New("real-failure"))

		// f3: gated on preResult, runs a step, then suspends.
		f3 := Go(ctx, "child3", func(childCtx Context) (string, error) {
			<-gate
			_, err := Step(childCtx, "trailing-step", func(_ StepContext) (string, error) {
				return "trailing", nil
			})
			if err != nil {
				return "", err
			}
			cb, err := CreateCallback[string](childCtx, "cb3")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})
		f3.preResult = func() { close(gate) }

		_, err := All(ctx, "all-err-drain", []*Future[string]{f1, f2, f3})
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("All error = %v, want errSuspendExecution", err)
	}
	assertStepCheckpointed(t, fake, "trailing-step")
}

// TestAllFailsFastBeforeUnresolved verifies that a non-suspension error
// observed before any suspension fails All immediately: the terminal error
// is returned without awaiting a later unresolved future. The unresolved
// future records any Result access and settles itself from its preResult
// hook, so a fail-fast regression surfaces as an assertion failure rather
// than a hang.
func TestAllFailsFastBeforeUnresolved(t *testing.T) {
	fake := &fakeLambda{}
	var laterAwaited atomic.Bool

	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFailedFuture[string](errors.New("terminal-failure"))
		f2 := newFuture[string]()
		f2.preResult = func() {
			laterAwaited.Store(true)
			f2.settle("late", nil)
		}

		_, err := All(ctx, "all-fail-fast", []*Future[string]{f1, f2})
		if err == nil {
			return "unexpected-success", nil
		}
		var childErr *ChildContextError
		if errors.As(err, &childErr) {
			return "failed-fast: " + childErr.Name, nil
		}
		return "other-err", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"failed-fast: all-fail-fast\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if laterAwaited.Load() {
		t.Error("All awaited a later future after a pre-suspension terminal error")
	}
}

// TestAllSettledSuspendThenTerminalPropagatesSuspension verifies that with
// three futures, AllSettled drains past a suspension and propagates it even
// when terminal outcomes follow. Child 2 is released specifically when
// AllSettled attempts f2.Result, so its step can only run if the drain
// continues past f1's suspension. The final future's preResult hook proves
// AllSettled awaited every future, including the last one, before
// returning.
func TestAllSettledSuspendThenTerminalPropagatesSuspension(t *testing.T) {
	fake := &fakeLambda{}
	gate := make(chan struct{})
	errCh := make(chan error, 1)
	var f3Awaited atomic.Bool

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb1")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})
		f2 := Go(ctx, "settler", func(childCtx Context) (string, error) {
			<-gate
			return Step(childCtx, "settle-step", func(_ StepContext) (string, error) {
				return "settled", nil
			})
		})
		f2.preResult = func() { close(gate) }
		f3 := newFailedFuture[string](errors.New("settled-failure"))
		f3.preResult = func() { f3Awaited.Store(true) }

		_, err := AllSettled(ctx, "allsettled-drain", []*Future[string]{f1, f2, f3})
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("AllSettled error = %v, want errSuspendExecution", err)
	}
	if !f3Awaited.Load() {
		t.Error("AllSettled returned without awaiting the final future")
	}
	assertStepCheckpointed(t, fake, "settle-step")
}

// TestAnyEarlyWinnerNoSuspension verifies that a success observed before
// any suspension returns immediately: Any resolves with the winner while a
// sibling future is still unsettled.
func TestAnyEarlyWinnerNoSuspension(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		// f1 stays unsettled until after Any returns; f2 is a
		// pre-settled success.
		f1 := newFuture[string]()
		f2 := newSettledFuture("won", nil)

		val, err := Any(ctx, "any-early", []*Future[string]{f1, f2})
		// Settle f1 so the join goroutine awaiting it unwinds.
		f1.settle("late", nil)
		if err != nil {
			return "", err
		}
		return val, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"won\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// TestRaceEarlyTerminalNoSuspension verifies that a terminal outcome
// observed before any suspension returns immediately: Race settles with a
// pre-settled error while a sibling future is still unsettled.
func TestRaceEarlyTerminalNoSuspension(t *testing.T) {
	fake := &fakeLambda{}
	errCh := make(chan error, 1)
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := newFuture[string]()
		f2 := newFailedFuture[string](errors.New("race-loser"))

		_, err := Race(ctx, "race-early", []*Future[string]{f1, f2})
		// Settle f1 so the join goroutine awaiting it unwinds.
		f1.settle("late", nil)
		errCh <- err
		return "caught", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	err := <-errCh
	if err == nil || errors.Is(err, errSuspendExecution) {
		t.Fatalf("Race error = %v, want the pre-settled terminal error", err)
	}
	if want := `durable: child context "race-early" failed: race-loser`; err.Error() != want {
		t.Errorf("Race error = %q, want %q", err.Error(), want)
	}
}

// TestAnySuspendThenWinnerPropagatesSuspension verifies that with three
// futures, Any keeps draining once a suspension is observed and propagates
// it even when a success and a real error follow. The interleaving is
// forced through the observation hook: f2 and f3 settle only after the
// receive loop has observed f1's suspension, so their outcomes always
// arrive in drain mode. The outcomes recorded at the moment Any returns
// prove all three futures were observed before it propagated the
// suspension.
func TestAnySuspendThenWinnerPropagatesSuspension(t *testing.T) {
	fake := &fakeLambda{}
	suspendObserved, rec := setCombinatorObserve(t)
	errCh := make(chan error, 1)
	outcomesCh := make(chan []error, 1)

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		// f1: creates a pending callback and awaits it, suspending.
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})

		// f2: succeeds via a step only after the suspension is observed.
		f2 := Go(ctx, "winner", func(childCtx Context) (string, error) {
			<-suspendObserved
			return Step(childCtx, "win-step", func(_ StepContext) (string, error) {
				return "won", nil
			})
		})

		// f3: fails with a real error only after the suspension is
		// observed.
		f3 := newFuture[string]()
		go func() {
			<-suspendObserved
			f3.settle("", errors.New("late-failure"))
		}()

		_, err := Any(ctx, "any-suspend-first", []*Future[string]{f1, f2, f3})
		outcomesCh <- rec.snapshot()
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("Any error = %v, want errSuspendExecution", err)
	}
	// Every future's outcome was observed before Any returned.
	assertDrainedOutcomes(t, <-outcomesCh, 3)
	// The drain let the winner branch finish its step before Any returned.
	assertStepCheckpointed(t, fake, "win-step")
}

// TestAnyThreeFutureSuspendThenFail verifies that Any with a suspension and
// only failures propagates the suspension: a suspended branch might still
// succeed on a later invocation, so it outranks a [*CombinatorError].
func TestAnyThreeFutureSuspendThenFail(t *testing.T) {
	fake := &fakeLambda{}
	errCh := make(chan error, 1)
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})
		f2 := newFailedFuture[string](errors.New("fail-1"))
		f3 := newFailedFuture[string](errors.New("fail-2"))

		_, err := Any(ctx, "any-suspend-fail", []*Future[string]{f1, f2, f3})
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("Any error = %v, want errSuspendExecution", err)
	}
}

// TestRaceSuspendThenTerminalPropagatesSuspension verifies that with three
// futures, Race keeps draining once a suspension is observed and propagates
// it even when a success and a real error follow. The interleaving is
// forced through the observation hook: f2 and f3 settle only after the
// receive loop has observed f1's suspension, so their outcomes always
// arrive in drain mode. The outcomes recorded at the moment Race returns
// prove all three futures were observed before it propagated the
// suspension.
func TestRaceSuspendThenTerminalPropagatesSuspension(t *testing.T) {
	fake := &fakeLambda{}
	suspendObserved, rec := setCombinatorObserve(t)
	errCh := make(chan error, 1)
	outcomesCh := make(chan []error, 1)

	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		// f1: creates a pending callback and awaits it, suspending.
		f1 := Go(ctx, "suspender", func(childCtx Context) (string, error) {
			cb, err := CreateCallback[string](childCtx, "cb")
			if err != nil {
				return "", err
			}
			return cb.Result()
		})

		// f2: succeeds via a step only after the suspension is observed.
		f2 := Go(ctx, "terminal", func(childCtx Context) (string, error) {
			<-suspendObserved
			return Step(childCtx, "t-step", func(_ StepContext) (string, error) {
				return "terminal-result", nil
			})
		})

		// f3: fails with a real error only after the suspension is
		// observed.
		f3 := newFuture[string]()
		go func() {
			<-suspendObserved
			f3.settle("", errors.New("late-failure"))
		}()

		_, err := Race(ctx, "race-suspend-first", []*Future[string]{f1, f2, f3})
		outcomesCh <- rec.snapshot()
		errCh <- err
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Fatalf("response = %s, want %s", resp, want)
	}
	if err := <-errCh; !errors.Is(err, errSuspendExecution) {
		t.Errorf("Race error = %v, want errSuspendExecution", err)
	}
	// Every future's outcome was observed before Race returned.
	assertDrainedOutcomes(t, <-outcomesCh, 3)
	// The drain let the terminal branch finish its step before Race
	// returned.
	assertStepCheckpointed(t, fake, "t-step")
}
