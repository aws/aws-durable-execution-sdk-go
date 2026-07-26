package durable

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

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
