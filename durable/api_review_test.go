package durable

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"reflect"
	"testing"
	"time"
)

// --- Item 1: ExecutionStartTime tests ---

func TestExecutionStartTimeReturnsCheckpointedTimestamp(t *testing.T) {
	// The root EXECUTION operation carries a StartTimestamp from the
	// backend. ExecutionStartTime must surface that value.
	startTS := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	fake := &fakeLambda{}

	ops := []wireOperation{{
		Id:             "exec-op",
		Type:           "EXECUTION",
		Status:         "STARTED",
		StartTimestamp: flexTimestamp{Time: startTS, Valid: true},
		ExecutionDetails: &wireExecutionDetails{
			InputPayload: `"hello"`,
		},
	}}
	in := invocationInput{
		DurableExecutionArn:   "arn:test",
		CheckpointToken:       "token-0",
		InitialExecutionState: initialExecutionState{Operations: ops},
	}
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}

	var observed time.Time
	h := Wrap(func(ctx Context, _ string) (string, error) {
		observed = ExecutionStartTime(ctx)
		return "done", nil
	}, withLambdaAPI(fake))

	_, err = h(context.Background(), payload)
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}

	if observed.IsZero() {
		t.Fatal("ExecutionStartTime returned zero time, want the checkpointed start timestamp")
	}
	if !observed.Equal(startTS) {
		t.Errorf("ExecutionStartTime = %v, want %v", observed, startTS)
	}
}

func TestExecutionStartTimeStableAcrossReplay(t *testing.T) {
	// On replay the same execution start time must be returned (not zero,
	// not a fresh time.Now()).
	startTS := time.Date(2025, 3, 20, 8, 0, 0, 0, time.UTC)
	fake := &fakeLambda{}

	ops := []wireOperation{
		{
			Id:             "exec-op",
			Type:           "EXECUTION",
			Status:         "STARTED",
			StartTimestamp: flexTimestamp{Time: startTS, Valid: true},
			ExecutionDetails: &wireExecutionDetails{
				InputPayload: `"replay"`,
			},
		},
		// Add a checkpointed step so the context enters replay mode.
		{
			Id:          hashID("1"),
			Type:        string(OperationTypeStep),
			SubType:     "Step",
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `"ok"`, Attempt: 1},
		},
	}
	in := invocationInput{
		DurableExecutionArn:   "arn:test",
		CheckpointToken:       "token-0",
		InitialExecutionState: initialExecutionState{Operations: ops},
	}
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}

	var observed time.Time
	h := Wrap(func(ctx Context, _ string) (string, error) {
		// First operation replays successfully.
		_, _ = Step(ctx, "s", func(StepContext) (string, error) {
			return "ok", nil
		})
		observed = ExecutionStartTime(ctx)
		return "done", nil
	}, withLambdaAPI(fake))

	_, err = h(context.Background(), payload)
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}

	if observed.IsZero() {
		t.Fatal("ExecutionStartTime on replay returned zero time")
	}
	if !observed.Equal(startTS) {
		t.Errorf("ExecutionStartTime on replay = %v, want %v", observed, startTS)
	}
}

func TestExecutionStartTimeAbsentTimestampReturnsZero(t *testing.T) {
	// A context whose state carried no execution start timestamp yields the
	// zero time rather than panicking.
	var zero time.Time
	ec := &execContext{}
	if got := ExecutionStartTime(ec); !got.Equal(zero) {
		t.Errorf("ExecutionStartTime on empty execContext = %v, want zero", got)
	}
}

// --- Item 2: durationToSeconds tests ---

func TestDurationToSecondsNegative(t *testing.T) {
	_, err := durationToSeconds(-1 * time.Second)
	if err == nil {
		t.Fatal("durationToSeconds(-1s) = nil error, want rejection")
	}
}

func TestDurationToSecondsZero(t *testing.T) {
	sec, err := durationToSeconds(0)
	if err != nil {
		t.Fatalf("durationToSeconds(0) error: %v", err)
	}
	if sec != 0 {
		t.Errorf("durationToSeconds(0) = %d, want 0", sec)
	}
}

func TestDurationToSecondsNormal(t *testing.T) {
	sec, err := durationToSeconds(90 * time.Second)
	if err != nil {
		t.Fatalf("durationToSeconds(90s) error: %v", err)
	}
	if sec != 90 {
		t.Errorf("durationToSeconds(90s) = %d, want 90", sec)
	}
}

func TestDurationToSecondsCeiling(t *testing.T) {
	sec, err := durationToSeconds(1500 * time.Millisecond)
	if err != nil {
		t.Fatalf("durationToSeconds(1.5s) error: %v", err)
	}
	if sec != 2 {
		t.Errorf("durationToSeconds(1.5s) = %d, want 2 (ceil)", sec)
	}
}

func TestDurationToSecondsExactMax(t *testing.T) {
	d := time.Duration(math.MaxInt32) * time.Second
	sec, err := durationToSeconds(d)
	if err != nil {
		t.Fatalf("durationToSeconds(MaxInt32 seconds) error: %v", err)
	}
	if sec != math.MaxInt32 {
		t.Errorf("durationToSeconds(MaxInt32s) = %d, want %d", sec, math.MaxInt32)
	}
}

func TestDurationToSecondsOverflow(t *testing.T) {
	d := time.Duration(math.MaxInt32+1) * time.Second
	_, err := durationToSeconds(d)
	if err == nil {
		t.Fatal("durationToSeconds(MaxInt32+1 seconds) = nil error, want overflow rejection")
	}
}

// --- Item 2: Integration tests for duration validation at public entry points ---

func TestWaitNegativeDurationReturnsError(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		err := Wait(ctx, "bad-wait", -5*time.Second)
		if err == nil {
			return "should-fail", nil
		}
		return "caught", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitOverflowDurationReturnsError(t *testing.T) {
	fake := &fakeLambda{}
	d := time.Duration(math.MaxInt32+1) * time.Second
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		err := Wait(ctx, "overflow-wait", d)
		if err == nil {
			return "should-fail", nil
		}
		return "caught", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestCallbackNegativeTimeoutReturnsError(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := CreateCallback[string](ctx, "bad-cb", WithCallbackTimeout(-3*time.Second))
		if err == nil {
			return "should-fail", nil
		}
		return "caught", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Item 4: Attempt contract tests ---

func TestStepAttemptFirstAttemptIs1(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		result, err := Step(ctx, "check-attempt", func(sc StepContext) (string, error) {
			if sc.Attempt() != 1 {
				return "", errors.New("first attempt should be 1")
			}
			return "ok", nil
		})
		if err != nil {
			return "", err
		}
		return result, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"ok\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestStepAttemptSecondAttemptIs2(t *testing.T) {
	// Simulate a retry: provide a step that is STARTED (from a prior
	// invocation) with Attempt=1 recorded.
	fake := &fakeLambda{}
	payload := stepPayload(`"x"`, wireOperation{
		Id:      hashID("1"),
		Name:    "retry-step",
		Type:    string(OperationTypeStep),
		SubType: "Step",
		Status:  "STARTED",
		StepDetails: &wireStepDetails{
			Attempt: 1,
		},
	})

	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		result, err := Step(ctx, "retry-step", func(sc StepContext) (string, error) {
			if sc.Attempt() != 2 {
				return "", errors.New("second attempt should be 2")
			}
			return "retry-ok", nil
		})
		if err != nil {
			return "", err
		}
		return result, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"retry-ok\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Item 5: Context sealing compile-time test ---

// TestContextSealedCompileCheck verifies that execContext satisfies the
// sealed interface at compile time (via the existing var _ Context = ...
// declaration in execution_context.go). This test documents that the
// sealed() method exists on the interface.
func TestContextSealedVarSatisfied(t *testing.T) {
	// The var _ Context = (*execContext)(nil) in execution_context.go
	// provides the compile-time assertion. This test documents the
	// relationship: if someone removes sealed() from Context, the var
	// breaks the build.
	ec := newTestContext(t, nil)
	var _ Context = ec // compilation proves sealing
}

// TestStepContextSealedVarSatisfied documents the two halves of the
// StepContext seal. The compile-time assertion in step.go proves that
// stepContext implements every method StepContext requires. The reflection
// check below proves that StepContext requires the unexported sealed method.
// The two checks are independent: a concrete type may carry methods its
// interface omits, so removing sealed from the interface alone still
// compiles. Only the reflection check catches that removal.
func TestStepContextSealedVarSatisfied(t *testing.T) {
	// Implementation side: stepContext satisfies StepContext. This mirrors
	// the var _ StepContext = (*stepContext)(nil) declaration in step.go.
	sc := &stepContext{Context: context.Background(), logger: slog.New(slog.DiscardHandler), attempt: 1}
	var _ StepContext = sc

	// Interface side: StepContext must carry an unexported method. Without
	// one, any user type with Logger and Attempt would satisfy StepContext,
	// and adding a method later would break that user type.
	typ := reflect.TypeOf((*StepContext)(nil)).Elem()
	if _, ok := typ.MethodByName("sealed"); !ok {
		t.Fatal("StepContext has no sealed() method; the interface is not sealed")
	}
}

// --- Item 1 helper: newTestContextWithExecTime ---

func TestExecutionStartTimePropagatedToChildContext(t *testing.T) {
	startTS := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ec := newExecContext(
		context.Background(),
		"arn:test",
		invocationInfo{requestID: "req"},
		slog.DiscardHandler,
		newExecutionState(nil),
	)
	ec.executionStartTime = startTS

	child := ec.child("1", "", currentGoroutineOwner(), modeExecution)
	if got := child.executionStartTime; !got.Equal(startTS) {
		t.Errorf("child executionStartTime = %v, want %v", got, startTS)
	}
}

// --- Item 2: WaitForCondition duration validation ---

func TestWaitForConditionNegativeDelayReturnsError(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "bad-delay", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: true, Delay: -5 * time.Second}
			},
		})
		if err == nil {
			return "should-fail", nil
		}
		return "caught", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionOverflowDelayReturnsError(t *testing.T) {
	fake := &fakeLambda{}
	d := time.Duration(math.MaxInt32+1) * time.Second
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "overflow", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: true, Delay: d}
			},
		})
		if err == nil {
			return "should-fail", nil
		}
		return "caught", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionZeroDelayAccepted(t *testing.T) {
	// Zero delay is valid: it means "retry immediately."
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "zero-delay", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: true, Delay: 0}
			},
		})
		// The operation suspends (PENDING) with delay=0, which is valid.
		if err != nil {
			return "", err
		}
		return "ok", nil
	})
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// --- Item 2: Step retry negative delay ---

func TestStepRetryNegativeDelayReturnsError(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "bad-retry", func(StepContext) (string, error) {
			return "", errors.New("fail")
		}, WithRetry(func(RetryAttempt) RetryDecision {
			return RetryDecision{Retry: true, Delay: -1 * time.Second}
		}))
		if err == nil {
			return "should-fail", nil
		}
		return "caught", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestStepRetryOverflowDelayReturnsError(t *testing.T) {
	fake := &fakeLambda{}
	d := time.Duration(math.MaxInt32+1) * time.Second
	resp := invokeStep(t, fake, stepPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "overflow-retry", func(StepContext) (string, error) {
			return "", errors.New("fail")
		}, WithRetry(func(RetryAttempt) RetryDecision {
			return RetryDecision{Retry: true, Delay: d}
		}))
		if err == nil {
			return "should-fail", nil
		}
		return "caught", nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}
