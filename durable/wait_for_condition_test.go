package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestWaitForConditionBasic(t *testing.T) {
	// Check increments state from 0; strategy continues until >= 3.
	// First cycle: 0->1, continue -> RETRY + suspend.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"3"`), func(ctx Context, event string) (string, error) {
		threshold, _ := strconv.Atoi(event)
		result, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= threshold {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("got %d updates, want 2", len(updates))
	}
	if updates[0].Action != OperationActionStart {
		t.Errorf("update[0].Action = %q, want START", updates[0].Action)
	}
	if aws.ToString(updates[0].SubType) != "WaitForCondition" {
		t.Errorf("SubType = %q, want WaitForCondition", aws.ToString(updates[0].SubType))
	}
	if updates[1].Action != OperationActionRetry {
		t.Errorf("update[1].Action = %q, want RETRY", updates[1].Action)
	}
	if aws.ToString(updates[1].Payload) != "1" {
		t.Errorf("payload = %q, want 1", aws.ToString(updates[1].Payload))
	}
	if aws.ToInt32(updates[1].StepOptions.NextAttemptDelaySeconds) != 1 {
		t.Errorf("delay = %d, want 1", aws.ToInt32(updates[1].StepOptions.NextAttemptDelaySeconds))
	}
}

func TestWaitForConditionResumesFromCheckpointedState(t *testing.T) {
	// Re-invocation: STARTED with Result "2" (previously checkpointed state).
	// Check 2->3, strategy stops -> SUCCEED.
	fake := &fakeLambda{}
	payload := stepPayload(`"3"`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 2, Result: "2"}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 3 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"3\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	found := false
	for _, u := range updates {
		if u.Action == OperationActionSucceed && aws.ToString(u.Payload) == "3" {
			found = true
		}
	}
	if !found {
		t.Error("expected SUCCEED checkpoint with payload 3")
	}
}

func TestWaitForConditionImmediateStop(t *testing.T) {
	// Condition already met on first check: no suspension.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"5"`), func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state, nil
		}, ConditionConfig[int]{
			InitialState: 5,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 5 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"5\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionIntermediateStateTooLarge(t *testing.T) {
	// The check returns a state whose serialized form exceeds the result
	// size limit while the strategy wants to continue. The RETRY path
	// must return *ResultTooLargeError instead of checkpointing the state.
	fake := &fakeLambda{}
	// A JSON string of this many characters serializes to two quote
	// bytes more, so it is one byte over the limit.
	oversized := strings.Repeat("x", resultSizeLimitBytes-1)
	var gotErr error
	strategyCalled := false
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "poll", func(_ StepContext, _ string) (string, error) {
			return oversized, nil
		}, ConditionConfig[string]{
			InitialState: "",
			WaitStrategy: func(_ string, _ int) WaitDecision {
				strategyCalled = true
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		gotErr = err
		if err != nil {
			return "", err
		}
		return "unexpected", nil
	})

	if !strategyCalled {
		t.Error("wait strategy was not consulted before the size check")
	}
	var rtlErr *ResultTooLargeError
	if !errors.As(gotErr, &rtlErr) {
		t.Fatalf("WaitForCondition error = %T (%v), want *ResultTooLargeError", gotErr, gotErr)
	}
	if rtlErr.Name != "poll" {
		t.Errorf("Name = %q, want %q", rtlErr.Name, "poll")
	}
	if want := resultSizeLimitBytes + 1; rtlErr.SizeBytes != want {
		t.Errorf("SizeBytes = %d, want %d", rtlErr.SizeBytes, want)
	}
	if rtlErr.LimitBytes != resultSizeLimitBytes {
		t.Errorf("LimitBytes = %d, want %d", rtlErr.LimitBytes, resultSizeLimitBytes)
	}
	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Errorf("response = %s, want FAILED status", resp)
	}

	// The oversized state must not reach the checkpoint API: the only
	// operation update for this operation is START.
	for _, u := range updateBatch(t, fake) {
		if aws.ToString(u.SubType) != "WaitForCondition" {
			continue
		}
		if u.Action != OperationActionStart {
			t.Errorf("unexpected WaitForCondition update action %q", u.Action)
		}
	}
}

func TestWaitForConditionIntermediateStateAtLimit(t *testing.T) {
	// A state whose serialized form is exactly the limit is not oversized:
	// the RETRY path checkpoints it as before.
	fake := &fakeLambda{}
	atLimit := strings.Repeat("x", resultSizeLimitBytes-2)
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "poll", func(_ StepContext, _ string) (string, error) {
			return atLimit, nil
		}, ConditionConfig[string]{
			InitialState: "",
			WaitStrategy: func(_ string, _ int) WaitDecision {
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		return "unexpected", nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	retried := false
	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionRetry && aws.ToString(u.SubType) == "WaitForCondition" {
			retried = true
			if got := len(aws.ToString(u.Payload)); got != resultSizeLimitBytes {
				t.Errorf("RETRY payload size = %d, want %d", got, resultSizeLimitBytes)
			}
		}
	}
	if !retried {
		t.Error("expected RETRY checkpoint for a state at the size limit")
	}
}

func TestWaitForConditionReplaySucceeded(t *testing.T) {
	// Terminal SUCCEEDED: deserialize stored result without re-executing.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Result: "42"}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "", func(_ StepContext, _ int) (int, error) {
			t.Fatal("check should not execute on replay")
			return 0, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"42\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionReplayFailed(t *testing.T) {
	// Terminal FAILED: return WaitForConditionError without re-executing.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "FAILED", &wireStepDetails{
			Attempt: 1,
			Error:   &wireStepError{ErrorType: "RuntimeError", ErrorMessage: "check failed"},
		}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "poll", func(_ StepContext, _ int) (int, error) {
			t.Fatal("check should not execute on replay")
			return 0, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		if err == nil {
			return "unexpected", nil
		}
		var condErr *WaitForConditionError
		if !errors.As(err, &condErr) {
			return "", err
		}
		return "caught: " + condErr.Name, nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"caught: poll\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionPending(t *testing.T) {
	// PENDING status: suspend immediately without re-executing.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "PENDING", &wireStepDetails{Attempt: 1, Result: "1"}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "", func(_ StepContext, _ int) (int, error) {
			t.Fatal("check should not execute on PENDING status")
			return 0, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionCheckError(t *testing.T) {
	// Check function returns an error: FAIL checkpoint, WaitForConditionError
	// returned.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "flaky", func(_ StepContext, _ int) (int, error) {
			return 0, errors.New("check function failed")
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		if err == nil {
			return "unexpected", nil
		}
		var condErr *WaitForConditionError
		if errors.As(err, &condErr) {
			return "caught: " + condErr.Name, nil
		}
		return "", err
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"caught: flaky\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	found := false
	for _, u := range updates {
		if u.Action == OperationActionFail && aws.ToString(u.SubType) == "WaitForCondition" {
			found = true
			// The checkpointed ErrorType is the cause's concrete type
			// name, not the SDK wrapper type. errors.New causes map to
			// "Error" on the wire.
			if got := aws.ToString(u.Error.ErrorType); got != "Error" {
				t.Errorf("checkpointed ErrorType = %q, want %q", got, "Error")
			}
			if got := aws.ToString(u.Error.ErrorMessage); got != "check function failed" {
				t.Errorf("checkpointed ErrorMessage = %q, want %q", got, "check function failed")
			}
		}
	}
	if !found {
		t.Error("expected FAIL checkpoint for WaitForCondition")
	}
}

// conditionCheckError is a named error type used to pin the checkpointed
// wire ErrorType for wait-for-condition failures.
type conditionCheckError struct{ msg string }

func (e *conditionCheckError) Error() string { return e.msg }

func TestWaitForConditionCheckErrorTypePinned(t *testing.T) {
	// The checkpointed operation ErrorType for a condition failure must be
	// the cause's concrete type name: WaitForConditionError wrapping is
	// applied only on the Go-return side, never on the wire.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "pinned", func(_ StepContext, _ int) (int, error) {
			return 0, &conditionCheckError{msg: "typed check failure"}
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		var condErr *WaitForConditionError
		if !errors.As(err, &condErr) {
			t.Errorf("returned error = %T, want *WaitForConditionError", err)
		}
		// The cause is a stand-in: the check error's type is exposed by
		// name, never as the original value.
		if condErr != nil && condErr.ErrorType != "conditionCheckError" {
			t.Errorf("WaitForConditionError.ErrorType = %q, want %q", condErr.ErrorType, "conditionCheckError")
		}
		var cause *conditionCheckError
		if errors.As(err, &cause) {
			t.Error("original check error reachable through Unwrap chain; the cause must be a stand-in")
		}
		return "done", nil
	})

	updates := updateBatch(t, fake)
	found := false
	for _, u := range updates {
		if u.Action == OperationActionFail && aws.ToString(u.SubType) == "WaitForCondition" {
			found = true
			if got := aws.ToString(u.Error.ErrorType); got != "conditionCheckError" {
				t.Errorf("checkpointed ErrorType = %q, want %q", got, "conditionCheckError")
			}
		}
	}
	if !found {
		t.Error("expected FAIL checkpoint for WaitForCondition")
	}
}

func TestWaitForConditionCheckPanic(t *testing.T) {
	// Check function panics: treated as error, FAIL checkpoint.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "panicker", func(_ StepContext, _ int) (int, error) {
			panic("kaboom")
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		if err == nil {
			return "unexpected", nil
		}
		return "caught", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"caught\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionWithName(t *testing.T) {
	// Verify that the name flows into checkpoint updates.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "poll-status", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 1 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	updates := updateBatch(t, fake)
	for _, u := range updates {
		if aws.ToString(u.Name) != "poll-status" {
			t.Errorf("update Name = %q, want poll-status", aws.ToString(u.Name))
		}
	}
}

func TestWaitForConditionCustomSerdes(t *testing.T) {
	// Custom serdes: verify it's used for serialization of state.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "", func(_ StepContext, state string) (string, error) {
			return state + "x", nil
		}, ConditionConfig[string]{
			InitialState: "",
			WaitStrategy: func(state string, _ int) WaitDecision {
				if len(state) >= 2 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
			Serdes: &uppercaseSerdes{},
		})
		return result, err
	})

	// First cycle: initial "" -> check "x" -> serialize "X" ->
	// deserialize "x" -> strategy continues (len 1 < 2) -> RETRY + suspend.
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	for _, u := range updates {
		if u.Action == OperationActionRetry {
			if aws.ToString(u.Payload) != `"X"` {
				t.Errorf("payload = %q, want %q", aws.ToString(u.Payload), `"X"`)
			}
		}
	}
}

func TestWaitForConditionCustomSerdesResume(t *testing.T) {
	// Resume from a checkpoint with custom-serialized state.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 1, Result: `"X"`}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		return WaitForCondition(ctx, "", func(_ StepContext, state string) (string, error) {
			return state + "x", nil
		}, ConditionConfig[string]{
			InitialState: "",
			WaitStrategy: func(state string, _ int) WaitDecision {
				if len(state) >= 2 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
			Serdes: &uppercaseSerdes{},
		})
	})

	// State "X" deserialized to "x", check "x"+"x"="xx", serialize "XX",
	// deserialize "xx", strategy stops -> SUCCEED. Return "xx".
	if want := `{"Status":"SUCCEEDED","Result":"\"xx\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionNullState(t *testing.T) {
	// State is nil/null: check returns nil, strategy stops immediately.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "", func(_ StepContext, state json.RawMessage) (json.RawMessage, error) {
			return state, nil
		}, ConditionConfig[json.RawMessage]{
			InitialState: nil,
			WaitStrategy: func(_ json.RawMessage, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		if err != nil {
			return "", err
		}
		return string(result), nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"null\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionComplexObject(t *testing.T) {
	type pollState struct {
		Status   string `json:"status"`
		Attempts int    `json:"attempts"`
	}

	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "", func(_ StepContext, state pollState) (pollState, error) {
			state.Attempts++
			if state.Attempts >= 2 {
				state.Status = "DONE"
			}
			return state, nil
		}, ConditionConfig[pollState]{
			InitialState: pollState{Status: "PENDING", Attempts: 0},
			WaitStrategy: func(state pollState, _ int) WaitDecision {
				if state.Status == "DONE" {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		return "", err
	})

	// First cycle: {PENDING,0}->{PENDING,1}, strategy continues -> suspend.
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionFixedDelay(t *testing.T) {
	// Verify the delay from the wait strategy flows into the checkpoint.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 2 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: 5 * time.Second}
			},
		})
		return "", err
	})

	updates := updateBatch(t, fake)
	for _, u := range updates {
		if u.Action == OperationActionRetry {
			if aws.ToInt32(u.StepOptions.NextAttemptDelaySeconds) != 5 {
				t.Errorf("delay = %d, want 5", aws.ToInt32(u.StepOptions.NextAttemptDelaySeconds))
			}
		}
	}
}

func TestWaitForConditionMaxAttempts(t *testing.T) {
	// First attempt: check runs, strategy says continue (attempt 1 < 3).
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "limited", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, attempt int) WaitDecision {
				if attempt >= 3 {
					return WaitDecision{Continue: false, Err: errors.New("max attempts exceeded")}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err == nil {
			return "unexpected", nil
		}
		var condErr *WaitForConditionError
		if errors.As(err, &condErr) {
			return "failed: " + condErr.Name, nil
		}
		return "", err
	})

	// Attempt 1: 0->1, strategy continues -> RETRY + suspend.
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionMaxAttemptsTerminal(t *testing.T) {
	// 3rd attempt: STARTED with Attempt=2. Check runs, strategy returns Err.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 2, Result: "2"}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "limited", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, attempt int) WaitDecision {
				if attempt >= 3 {
					return WaitDecision{Continue: false, Err: errors.New("max attempts exceeded")}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err == nil {
			return "unexpected", nil
		}
		return "failed", nil
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"failed\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	found := false
	for _, u := range updates {
		if u.Action == OperationActionFail {
			found = true
			if aws.ToString(u.Error.ErrorMessage) != "max attempts exceeded" {
				t.Errorf("error message = %q", aws.ToString(u.Error.ErrorMessage))
			}
			// Checkpointed ErrorType stays the cause's concrete type
			// name (errors.New maps to "Error"), not the wrapper type.
			if got := aws.ToString(u.Error.ErrorType); got != "Error" {
				t.Errorf("checkpointed ErrorType = %q, want %q", got, "Error")
			}
		}
	}
	if !found {
		t.Error("expected FAIL checkpoint")
	}
}

func TestWaitForConditionMultipleSequential(t *testing.T) {
	// Two sequential WaitForCondition operations: distinct IDs.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		first, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 1 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		second, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: first,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 2 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(second), nil
	})

	// First WFC: 0->1, stop -> SUCCEED. Second WFC: 1->2, stop -> SUCCEED.
	if want := `{"Status":"SUCCEEDED","Result":"\"2\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	var startIDs []string
	for _, u := range updates {
		if u.Action == OperationActionStart {
			startIDs = append(startIDs, aws.ToString(u.Id))
		}
	}
	if len(startIDs) != 2 {
		t.Fatalf("got %d STARTs, want 2", len(startIDs))
	}
	if startIDs[0] == startIDs[1] {
		t.Error("sequential WaitForCondition ops have the same ID")
	}
}

func TestWaitForConditionDelayCeiling(t *testing.T) {
	// Sub-second delays round up to 1 second minimum.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 2 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: 100 * time.Millisecond}
			},
		})
		return "", err
	})

	updates := updateBatch(t, fake)
	for _, u := range updates {
		if u.Action == OperationActionRetry {
			if aws.ToInt32(u.StepOptions.NextAttemptDelaySeconds) != 1 {
				t.Errorf("delay = %d, want 1 (minimum)", aws.ToInt32(u.StepOptions.NextAttemptDelaySeconds))
			}
		}
	}
}

func TestWaitForConditionThenStep(t *testing.T) {
	// WaitForCondition result feeds a subsequent Step (immediate stop).
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		pollResult, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(state int, _ int) WaitDecision {
				if state >= 2 {
					return WaitDecision{Continue: false}
				}
				return WaitDecision{Continue: true, Delay: time.Second}
			},
		})
		if err != nil {
			return "", err
		}
		return Step(ctx, "", func(_ StepContext) (string, error) {
			return strconv.Itoa(pollResult * 10), nil
		})
	})

	// First WFC: 0->1, continue -> suspend.
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestWaitForConditionAfterSuspension(t *testing.T) {
	// If suspension has already fired, WaitForCondition fails fast.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		// Wait fires suspension.
		_ = Wait(ctx, "w", time.Second)
		// WaitForCondition should fail fast with errSuspendExecution.
		_, err := WaitForCondition(ctx, "", func(_ StepContext, state int) (int, error) {
			return state, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		return "", err
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

// uppercaseSerdes is a test serdes that uppercases string state on
// serialize and lowercases on deserialize.
type uppercaseSerdes struct{}

func (s *uppercaseSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(b))
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			result[i] = c - 32
		} else {
			result[i] = c
		}
	}
	return result, nil
}

func (s *uppercaseSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	lower := make([]byte, len(data))
	for i, c := range data {
		if c >= 'A' && c <= 'Z' {
			lower[i] = c + 32
		} else {
			lower[i] = c
		}
	}
	return json.Unmarshal(lower, v)
}

func TestWaitForConditionStateDeserFailure(t *testing.T) {
	// A checkpointed state that cannot be deserialized fails the
	// operation with a SerdesError; it must not silently fall back to
	// the initial state.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 1, Result: "not-json"}),
	)
	var got error
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "corrupt", func(_ StepContext, state int) (int, error) {
			t.Error("check must not execute when checkpointed state cannot be deserialized")
			return state, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		got = err
		return "", err
	})

	var serdesErr *SerdesError
	if !errors.As(got, &serdesErr) {
		t.Fatalf("error = %v (%T), want *SerdesError", got, got)
	}
	if serdesErr.Operation != "corrupt" {
		t.Errorf("SerdesError.Operation = %q, want %q", serdesErr.Operation, "corrupt")
	}
	if serdesErr.Direction != "unmarshal" {
		t.Errorf("SerdesError.Direction = %q, want %q", serdesErr.Direction, "unmarshal")
	}
	var jsonErr *json.SyntaxError
	if !errors.As(got, &jsonErr) {
		t.Errorf("underlying cause %v not reachable via errors.As through Unwrap", serdesErr.Err)
	}
	var parsed invocationResponse
	if err := json.Unmarshal([]byte(resp), &parsed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if parsed.Status != invocationFailed {
		t.Errorf("response status = %q, want FAILED", parsed.Status)
	}
}

func TestWaitForConditionDefaultWaitStrategyInitial(t *testing.T) {
	// A zero ConditionConfig (nil WaitStrategy) must not panic: the
	// default strategy applies. First attempt: base delay 5s with full
	// jitter, so the checkpointed delay is within [1s, 5s].
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "default-strategy", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	var retry *OperationUpdate
	for i, u := range updates {
		if u.Action == OperationActionRetry {
			retry = &updates[i]
		}
	}
	if retry == nil {
		t.Fatal("expected a RETRY checkpoint from the default wait strategy")
	}
	delay := aws.ToInt32(retry.StepOptions.NextAttemptDelaySeconds)
	if delay < 1 || delay > 5 {
		t.Errorf("attempt 1 delay = %ds, want within [1s, 5s] (5s base, full jitter)", delay)
	}
	if aws.ToString(retry.Payload) != "1" {
		t.Errorf("payload = %q, want 1", aws.ToString(retry.Payload))
	}
}

func TestWaitForConditionDefaultWaitStrategyReplay(t *testing.T) {
	// Re-invocation resuming a checkpointed operation: the default
	// strategy also applies on replay. With 3 completed attempts this is
	// attempt 4: base delay 5 × 1.5³ = 16.875s, full jitter, rounded, so
	// the checkpointed delay is within [1s, 17s].
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 3, Result: "3"}),
	)
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		result, err := WaitForCondition(ctx, "default-strategy", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{})
		if err != nil {
			return "", err
		}
		return strconv.Itoa(result), nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	var retry *OperationUpdate
	for i, u := range updates {
		if u.Action == OperationActionRetry {
			retry = &updates[i]
		}
	}
	if retry == nil {
		t.Fatal("expected a RETRY checkpoint from the default wait strategy")
	}
	delay := aws.ToInt32(retry.StepOptions.NextAttemptDelaySeconds)
	if delay < 1 || delay > 17 {
		t.Errorf("attempt 4 delay = %ds, want within [1s, 17s] (16.875s base, full jitter)", delay)
	}
	if aws.ToString(retry.Payload) != "4" {
		t.Errorf("payload = %q, want 4 (state carried from checkpoint)", aws.ToString(retry.Payload))
	}
}

func TestWaitForConditionDefaultWaitStrategyDelayCap(t *testing.T) {
	// Deep into the schedule the base delay caps at 300s: with 11
	// completed attempts this is attempt 12, base 5 × 1.5¹¹ ≈ 432s
	// capped to 300s, full jitter, so the delay is within [1s, 300s].
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 11, Result: "11"}),
	)
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "default-strategy", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{})
		return "", err
	})

	updates := updateBatch(t, fake)
	var retry *OperationUpdate
	for i, u := range updates {
		if u.Action == OperationActionRetry {
			retry = &updates[i]
		}
	}
	if retry == nil {
		t.Fatal("expected a RETRY checkpoint from the default wait strategy")
	}
	delay := aws.ToInt32(retry.StepOptions.NextAttemptDelaySeconds)
	if delay < 1 || delay > 300 {
		t.Errorf("attempt 12 delay = %ds, want within [1s, 300s] (capped base, full jitter)", delay)
	}
}

func TestWaitForConditionDefaultWaitStrategyExhaustion(t *testing.T) {
	// After 59 completed attempts, attempt 60 exhausts the default
	// strategy: the operation fails with a max-attempts error.
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 59, Result: "59"}),
	)
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "default-strategy", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{})
		got = err
		return "", err
	})

	var wfcErr *WaitForConditionError
	if !errors.As(got, &wfcErr) {
		t.Fatalf("error = %v (%T), want *WaitForConditionError", got, got)
	}
	if wfcErr.Attempts != 60 {
		t.Errorf("Attempts = %d, want 60", wfcErr.Attempts)
	}
	if !strings.Contains(got.Error(), "exceeded maximum attempts (60)") {
		t.Errorf("error %q does not mention exceeding 60 attempts", got)
	}

	updates := updateBatch(t, fake)
	failed := false
	for _, u := range updates {
		if u.Action == OperationActionFail {
			failed = true
		}
	}
	if !failed {
		t.Error("expected a FAIL checkpoint when the default strategy exhausts")
	}
}

func TestWaitForConditionDefaultStrategyConstants(t *testing.T) {
	// Pin the documented default strategy parameters. A change to any
	// constant fails this test, catching regressions that the jittered
	// integration tests would miss.
	if defaultConditionMaxAttempts != 60 {
		t.Errorf("defaultConditionMaxAttempts = %d, want 60", defaultConditionMaxAttempts)
	}
	if defaultConditionInitialDelay != 5*time.Second {
		t.Errorf("defaultConditionInitialDelay = %v, want 5s", defaultConditionInitialDelay)
	}
	if defaultConditionMaxDelay != 300*time.Second {
		t.Errorf("defaultConditionMaxDelay = %v, want 300s", defaultConditionMaxDelay)
	}
	if defaultConditionBackoffRate != 1.5 {
		t.Errorf("defaultConditionBackoffRate = %v, want 1.5", defaultConditionBackoffRate)
	}
	if defaultConditionJitterStrategy != JitterFull {
		t.Errorf("defaultConditionJitterStrategy = %q, want %q", defaultConditionJitterStrategy, JitterFull)
	}
}

func TestWaitForConditionDefaultStrategyDelayBounds(t *testing.T) {
	// Verify that the default wait strategy produces delays with exact
	// upper bounds matching the formula: min(5 × 1.5^(attempt-1), 300),
	// rounded to whole seconds. Full jitter means the lower bound is 1s.
	// Running 200 samples per attempt ensures that the upper bound is
	// tight (probability of never hitting max ≈ (1-1/base)^200, which is
	// negligible for base ≤ 5).
	type testCase struct {
		attempt  int
		maxDelay time.Duration // expected tight upper bound
	}
	cases := []testCase{
		{1, 5 * time.Second},    // base 5
		{2, 8 * time.Second},    // base 7.5 → rounds to 8
		{3, 11 * time.Second},   // base 11.25 → rounds to 11
		{4, 17 * time.Second},   // base 16.875 → rounds to 17
		{10, 192 * time.Second}, // base 5 × 1.5^9 ≈ 192.2 → rounds to 192
		{15, 300 * time.Second}, // base 5 × 1.5^14 ≈ 1458, capped at 300
	}

	for _, tc := range cases {
		var maxObserved time.Duration
		var minObserved = time.Hour
		// Use more samples for large ranges to ensure coverage of [1s, max].
		samples := 200
		if tc.maxDelay > 30*time.Second {
			samples = 2000
		}
		for range samples {
			decision := defaultConditionWaitStrategy[int]()(0, tc.attempt)
			if !decision.Continue {
				t.Fatalf("attempt %d: expected Continue=true", tc.attempt)
			}
			if decision.Delay < time.Second {
				t.Fatalf("attempt %d: delay %v < 1s minimum", tc.attempt, decision.Delay)
			}
			if decision.Delay > tc.maxDelay {
				t.Fatalf("attempt %d: delay %v exceeds expected max %v", tc.attempt, decision.Delay, tc.maxDelay)
			}
			if decision.Delay > maxObserved {
				maxObserved = decision.Delay
			}
			if decision.Delay < minObserved {
				minObserved = decision.Delay
			}
		}
		// Verify the observed range uses most of the space.
		if tc.maxDelay >= 5*time.Second && maxObserved < tc.maxDelay-2*time.Second {
			t.Errorf("attempt %d: max observed %v is far below expected max %v — jitter range suspiciously narrow",
				tc.attempt, maxObserved, tc.maxDelay)
		}
		// For full jitter over [1, max/second], with enough samples
		// we should observe values close to 1s (the floor).
		if tc.maxDelay >= 5*time.Second && minObserved > 3*time.Second {
			t.Errorf("attempt %d: min observed %v is suspiciously high — expected full jitter to reach near 1s",
				tc.attempt, minObserved)
		}
	}
}

func TestWaitForConditionDefaultStrategyExhaustionBoundary(t *testing.T) {
	// Attempt 59 must continue; attempt 60 must fail with the max-
	// attempts error. This pins the exhaustion point.
	decision59 := defaultConditionWaitStrategy[int]()(0, 59)
	if !decision59.Continue {
		t.Fatal("attempt 59: expected Continue=true (not exhausted yet)")
	}
	if decision59.Delay < time.Second {
		t.Fatalf("attempt 59: delay %v < 1s", decision59.Delay)
	}

	decision60 := defaultConditionWaitStrategy[int]()(0, 60)
	if decision60.Continue {
		t.Fatal("attempt 60: expected Continue=false (exhausted)")
	}
	if decision60.Err == nil {
		t.Fatal("attempt 60: expected non-nil Err")
	}
	if !strings.Contains(decision60.Err.Error(), "60") {
		t.Errorf("attempt 60 error = %q, want mention of 60", decision60.Err)
	}

	// Attempt 61 also fails (boundary is at 60).
	decision61 := defaultConditionWaitStrategy[int]()(0, 61)
	if decision61.Continue {
		t.Fatal("attempt 61: expected Continue=false")
	}
}

func TestWaitForConditionDefaultStrategyCheckpointDelay(t *testing.T) {
	// End-to-end: verify the checkpointed delay on initial execution
	// (attempt 1) and on replay (attempt 4, STARTED status) have tight
	// upper bounds derived from the exact formula. This catches changes
	// to the default strategy that would persist different timing values
	// in checkpoints.

	// Initial execution: attempt 1, base delay = 5s.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "pinned", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{})
		return "", err
	})
	updates := updateBatch(t, fake)
	var retry *OperationUpdate
	for i, u := range updates {
		if u.Action == OperationActionRetry {
			retry = &updates[i]
		}
	}
	if retry == nil {
		t.Fatal("attempt 1: no RETRY checkpoint")
	}
	delay1 := aws.ToInt32(retry.StepOptions.NextAttemptDelaySeconds)
	// Exact upper bound: round(5) = 5.
	if delay1 < 1 || delay1 > 5 {
		t.Errorf("attempt 1 delay = %ds, want in [1, 5] (base=5, full jitter)", delay1)
	}

	// Replay: attempt 4, base delay = 5 × 1.5³ = 16.875 → rounds to 17.
	fake = &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 3, Result: "3"}),
	)
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "pinned", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{})
		return "", err
	})
	updates = updateBatch(t, fake)
	retry = nil
	for i, u := range updates {
		if u.Action == OperationActionRetry {
			retry = &updates[i]
		}
	}
	if retry == nil {
		t.Fatal("attempt 4: no RETRY checkpoint")
	}
	delay4 := aws.ToInt32(retry.StepOptions.NextAttemptDelaySeconds)
	// Exact upper bound: round(16.875) = 17.
	if delay4 < 1 || delay4 > 17 {
		t.Errorf("attempt 4 delay = %ds, want in [1, 17] (base=16.875, full jitter)", delay4)
	}

	// Replay: attempt 12, base = 5 × 1.5¹¹ ≈ 432.7, capped at 300.
	fake = &fakeLambda{}
	payload = stepPayload(`""`,
		checkpointedStep("1", "STARTED", &wireStepDetails{Attempt: 11, Result: "11"}),
	)
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "pinned", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{})
		return "", err
	})
	updates = updateBatch(t, fake)
	retry = nil
	for i, u := range updates {
		if u.Action == OperationActionRetry {
			retry = &updates[i]
		}
	}
	if retry == nil {
		t.Fatal("attempt 12: no RETRY checkpoint")
	}
	delay12 := aws.ToInt32(retry.StepOptions.NextAttemptDelaySeconds)
	// Exact upper bound: min(432.7, 300) = 300.
	if delay12 < 1 || delay12 > 300 {
		t.Errorf("attempt 12 delay = %ds, want in [1, 300] (capped at 300, full jitter)", delay12)
	}
}
