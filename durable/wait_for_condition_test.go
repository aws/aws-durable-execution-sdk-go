package durable

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
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
	if updates[0].Action != types.OperationActionStart {
		t.Errorf("update[0].Action = %q, want START", updates[0].Action)
	}
	if aws.ToString(updates[0].SubType) != "WaitForCondition" {
		t.Errorf("SubType = %q, want WaitForCondition", aws.ToString(updates[0].SubType))
	}
	if updates[1].Action != types.OperationActionRetry {
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
		if u.Action == types.OperationActionSucceed && aws.ToString(u.Payload) == "3" {
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
	// Terminal FAILED: return StepError without re-executing.
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
		var stepErr *StepError
		if !errors.As(err, &stepErr) {
			return "", err
		}
		return "caught: " + stepErr.Name, nil
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
	// Check function returns an error: FAIL checkpoint, StepError returned.
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
		var stepErr *StepError
		if errors.As(err, &stepErr) {
			return "caught: " + stepErr.Name, nil
		}
		return "", err
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"caught: flaky\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}

	updates := updateBatch(t, fake)
	found := false
	for _, u := range updates {
		if u.Action == types.OperationActionFail && aws.ToString(u.SubType) == "WaitForCondition" {
			found = true
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
		if u.Action == types.OperationActionRetry {
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
		if u.Action == types.OperationActionRetry {
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
		var stepErr *StepError
		if errors.As(err, &stepErr) {
			return "failed: " + stepErr.Name, nil
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
		if u.Action == types.OperationActionFail {
			found = true
			if aws.ToString(u.Error.ErrorMessage) != "max attempts exceeded" {
				t.Errorf("error message = %q", aws.ToString(u.Error.ErrorMessage))
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
		if u.Action == types.OperationActionStart {
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
		if u.Action == types.OperationActionRetry {
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

func (s *uppercaseSerdes) Marshal(_ SerdesContext, v any) ([]byte, error) {
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

func (s *uppercaseSerdes) Unmarshal(_ SerdesContext, data []byte, v any) error {
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
