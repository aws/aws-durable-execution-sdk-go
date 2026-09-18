package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// stepPayload builds an invocation payload whose initial state contains the
// execution operation (carrying event) plus the given step operations.
func stepPayload(event string, steps ...wireOperation) []byte {
	ops := append([]wireOperation{{
		Id:               "exec-op",
		Status:           "STARTED",
		Type:             "EXECUTION",
		ExecutionDetails: &wireExecutionDetails{InputPayload: event},
	}}, steps...)
	in := invocationInput{
		DurableExecutionArn:   "arn:test",
		CheckpointToken:       "token-0",
		InitialExecutionState: initialExecutionState{Operations: ops},
	}
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	return b
}

// checkpointedStep is a shorthand for a step operation in the embedded
// state page, keyed by its wire (hashed) ID.
func checkpointedStep(positionalID, status string, details *wireStepDetails) wireOperation {
	return wireOperation{Id: hashID(positionalID), Status: status, StepDetails: details}
}

// invokeStep runs handler through the full durable invocation path against
// fake, returning the raw response.
func invokeStep(t *testing.T, fake *fakeLambda, payload []byte, handler Handler[string, string]) string {
	t.Helper()
	h := Wrap(handler, withLambdaAPI(fake))
	got, err := h(context.Background(), payload)
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	return string(got)
}

// updateBatch flattens the fake's recorded update batches for assertions.
func updateBatch(t *testing.T, fake *fakeLambda) []OperationUpdate {
	t.Helper()
	var updates []OperationUpdate
	for _, batch := range fake.gotUpdateBatches {
		updates = append(updates, batch...)
	}
	return updates
}

func assertStepUpdate(t *testing.T, u OperationUpdate, positionalID string, action OperationAction) {
	t.Helper()
	if got, want := aws.ToString(u.Id), hashID(positionalID); got != want {
		t.Errorf("update Id = %q, want hash of %q (%q)", got, positionalID, want)
	}
	if u.Type != OperationTypeStep {
		t.Errorf("update Type = %q, want STEP", u.Type)
	}
	if got := aws.ToString(u.SubType); got != "Step" {
		t.Errorf("update SubType = %q, want Step", got)
	}
	if u.Action != action {
		t.Errorf("update Action = %q, want %q", u.Action, action)
	}
}

func TestStepFirstRunSuccess(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"World"`), func(ctx Context, event string) (string, error) {
		return Step(ctx, "greet", func(StepContext) (string, error) {
			return "Hello, " + event + "!", nil
		})
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"Hello, World!\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("received %d updates, want 2 (START, SUCCEED)", len(updates))
	}
	assertStepUpdate(t, updates[0], "1", OperationActionStart)
	if got := aws.ToString(updates[0].Name); got != "greet" {
		t.Errorf("START Name = %q, want greet", got)
	}
	if updates[0].ParentId != nil {
		t.Errorf("root step ParentId = %q, want nil", aws.ToString(updates[0].ParentId))
	}
	assertStepUpdate(t, updates[1], "1", OperationActionSucceed)
	if got := aws.ToString(updates[1].Payload); got != `"Hello, World!"` {
		t.Errorf("SUCCEED Payload = %q, want %q", got, `"Hello, World!"`)
	}
}

func TestStepUnnamedOmitsName(t *testing.T) {
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "", func(StepContext) (string, error) { return "x", nil })
	})

	updates := updateBatch(t, fake)
	if len(updates) == 0 {
		t.Fatal("no updates received")
	}
	if updates[0].Name != nil {
		t.Errorf("unnamed step Name = %q, want nil", aws.ToString(updates[0].Name))
	}
}

func TestStepSequentialIDs(t *testing.T) {
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		first, err := Step(ctx, "first", func(StepContext) (string, error) { return "first", nil })
		if err != nil {
			return "", err
		}
		return Step(ctx, "second", func(StepContext) (string, error) { return first + "_second", nil })
	})

	updates := updateBatch(t, fake)
	if len(updates) != 4 {
		t.Fatalf("received %d updates, want 4", len(updates))
	}
	assertStepUpdate(t, updates[0], "1", OperationActionStart)
	assertStepUpdate(t, updates[1], "1", OperationActionSucceed)
	assertStepUpdate(t, updates[2], "2", OperationActionStart)
	assertStepUpdate(t, updates[3], "2", OperationActionSucceed)
}

func TestStepReplaySucceededReturnsCachedResult(t *testing.T) {
	fake := &fakeLambda{}
	executed := false
	resp := invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `"cached_value"`})),
		func(ctx Context, _ string) (string, error) {
			return Step(ctx, "s", func(StepContext) (string, error) {
				executed = true
				return "recomputed", nil
			})
		})

	if executed {
		t.Error("step body executed during replay of a SUCCEEDED step")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"cached_value\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("replay sent %d updates, want 0", n)
	}
}

func TestStepReplayFailedReturnsStepError(t *testing.T) {
	fake := &fakeLambda{}
	executed := false
	var gotErr error
	resp := invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "FAILED", &wireStepDetails{
			Attempt: 2,
			Error:   &wireStepError{ErrorType: "TransientError", ErrorMessage: "boom"},
		})),
		func(ctx Context, _ string) (string, error) {
			_, err := Step(ctx, "s", func(StepContext) (string, error) {
				executed = true
				return "", nil
			})
			gotErr = err
			// The error is caught and handled: execution continues
			// with a fallback (conformance 1-20 pattern).
			return "fallback_result", nil
		})

	if executed {
		t.Error("step body executed during replay of a FAILED step")
	}
	var stepErr *StepError
	if !errors.As(gotErr, &stepErr) {
		t.Fatalf("step error = %v, want *StepError", gotErr)
	}
	if stepErr.Attempts != 2 {
		t.Errorf("StepError.Attempts = %d, want 2", stepErr.Attempts)
	}
	if !strings.Contains(stepErr.Error(), "boom") {
		t.Errorf("StepError message %q does not carry checkpointed message", stepErr.Error())
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"fallback_result\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestStepNoRetryFailure(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			return "", errors.New("permanent failure")
		}, WithRetry(NoRetry()))
	})

	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Errorf("response = %s, want FAILED", resp)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("received %d updates, want 2 (START, FAIL)", len(updates))
	}
	assertStepUpdate(t, updates[1], "1", OperationActionFail)
	if updates[1].Error == nil {
		t.Fatal("FAIL update has no Error object")
	}
	if got := aws.ToString(updates[1].Error.ErrorMessage); got != "permanent failure" {
		t.Errorf("FAIL ErrorMessage = %q, want %q", got, "permanent failure")
	}
	if got := aws.ToString(updates[1].Error.ErrorType); got != "Error" {
		t.Errorf("FAIL ErrorType = %q, want Error", got)
	}
}

func TestStepRetrySchedulesAndSuspends(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			return "", errors.New("transient")
		}, WithRetry(MustNewRetryStrategy(RetryConfig{MaxAttempts: 4, InitialDelay: 2 * time.Second, Jitter: JitterNone})))
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("received %d updates, want 2 (START, RETRY)", len(updates))
	}
	assertStepUpdate(t, updates[1], "1", OperationActionRetry)
	if updates[1].StepOptions == nil {
		t.Fatal("RETRY update has no StepOptions")
	}
	if got := aws.ToInt32(updates[1].StepOptions.NextAttemptDelaySeconds); got != 2 {
		t.Errorf("NextAttemptDelaySeconds = %d, want 2", got)
	}
	if updates[1].Error == nil {
		t.Error("RETRY update has no Error object")
	}
}

func TestStepRetryStrategyReceivesAttempt(t *testing.T) {
	// The strategy sees the failing error and the 1-based attempt number
	// in the RetryAttempt it receives. Elapsed is not tracked yet, so it
	// is zero.
	fake := &fakeLambda{}
	cause := errors.New("transient")
	var got RetryAttempt
	var calls int
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			return "", cause
		}, WithRetry(func(a RetryAttempt) RetryDecision {
			calls++
			got = a
			return RetryDecision{}
		}))
	})

	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Errorf("response = %s, want FAILED", resp)
	}
	if calls != 1 {
		t.Fatalf("retry strategy called %d times, want 1", calls)
	}
	if !errors.Is(got.Err, cause) {
		t.Errorf("RetryAttempt.Err = %v, want %v", got.Err, cause)
	}
	if got.Attempt != 1 {
		t.Errorf("RetryAttempt.Attempt = %d, want 1", got.Attempt)
	}
	if got.Elapsed != 0 {
		t.Errorf("RetryAttempt.Elapsed = %v, want 0", got.Elapsed)
	}
}

func TestStepSuspensionWinsOverUserOutcome(t *testing.T) {
	// User code swallows the suspension error and returns success; the
	// invocation must still respond PENDING.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, _ = Step(ctx, "s", func(StepContext) (string, error) {
			return "", errors.New("transient")
		}, WithRetry(MustLinearBackoff(time.Second)))
		return "swallowed", nil
	})

	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
}

func TestStepAfterSuspensionFailsFast(t *testing.T) {
	fake := &fakeLambda{}
	var secondErr error
	// After the first step commits to PENDING, subsequent claims on the
	// same context fail. This prevents user code that swallows errors
	// from starting new operations.
	handlerDone := make(chan struct{})
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		defer close(handlerDone)
		_, _ = Step(ctx, "s", func(StepContext) (string, error) {
			return "", errors.New("transient")
		}, WithRetry(MustLinearBackoff(time.Second)))
		_, secondErr = Step(ctx, "after", func(StepContext) (string, error) {
			t.Error("step body ran after suspension")
			return "", nil
		})
		return "", secondErr
	})
	<-handlerDone

	if !errors.Is(secondErr, errSuspendExecution) {
		t.Errorf("step after suspension error = %v, want errSuspendExecution", secondErr)
	}
	// Only START and RETRY for the first step; nothing for the second.
	if n := len(updateBatch(t, fake)); n != 2 {
		t.Errorf("received %d updates, want 2", n)
	}
}

func TestStepPendingSuspends(t *testing.T) {
	// A PENDING step means a retry is scheduled and its timer has not
	// fired: the invocation suspends without touching the operation.
	fake := &fakeLambda{}
	executed := false
	resp := invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "PENDING", &wireStepDetails{Attempt: 1})),
		func(ctx Context, _ string) (string, error) {
			return Step(ctx, "s", func(StepContext) (string, error) {
				executed = true
				return "", nil
			})
		})

	if executed {
		t.Error("step body executed while PENDING")
	}
	if want := `{"Status":"PENDING"}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("received %d updates, want 0", n)
	}
}

func TestStepReadyReexecutesWithNextAttempt(t *testing.T) {
	// After a retry timer fires the step is READY; the next attempt
	// number derives from the checkpointed attempt count.
	fake := &fakeLambda{}
	var gotAttempt int
	strategy := func(a RetryAttempt) RetryDecision {
		gotAttempt = a.Attempt
		return RetryDecision{}
	}
	resp := invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "READY", &wireStepDetails{Attempt: 1})),
		func(ctx Context, _ string) (string, error) {
			return Step(ctx, "s", func(StepContext) (string, error) {
				return "", errors.New("fails again")
			}, WithRetry(strategy))
		})

	if gotAttempt != 2 {
		t.Errorf("retry strategy received attempt %d, want 2", gotAttempt)
	}
	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Errorf("response = %s, want FAILED", resp)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("received %d updates, want 2 (START, FAIL)", len(updates))
	}
	assertStepUpdate(t, updates[0], "1", OperationActionStart)
	assertStepUpdate(t, updates[1], "1", OperationActionFail)
}

func TestStepStartedAtLeastOnceReexecutes(t *testing.T) {
	// An interrupted attempt (STARTED with no outcome) re-executes under
	// the default at-least-once semantics, without a duplicate START.
	fake := &fakeLambda{}
	executed := false
	resp := invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "STARTED", nil)),
		func(ctx Context, _ string) (string, error) {
			return Step(ctx, "s", func(StepContext) (string, error) {
				executed = true
				return "recovered", nil
			})
		})

	if !executed {
		t.Error("interrupted step did not re-execute under at-least-once semantics")
	}
	if want := `{"Status":"SUCCEEDED","Result":"\"recovered\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 1 {
		t.Fatalf("received %d updates, want 1 (SUCCEED only)", len(updates))
	}
	assertStepUpdate(t, updates[0], "1", OperationActionSucceed)
}

func TestStepStartedAtMostOnceInterrupted(t *testing.T) {
	tests := []struct {
		name        string
		retry       RetryStrategy
		wantStatus  string
		wantAction  OperationAction
		wantUpdates int
	}{
		{
			name:        "no retry fails permanently",
			retry:       NoRetry(),
			wantStatus:  `"Status":"FAILED"`,
			wantAction:  OperationActionFail,
			wantUpdates: 1,
		},
		{
			name:        "retry schedules next attempt",
			retry:       MustLinearBackoff(time.Second),
			wantStatus:  `"Status":"PENDING"`,
			wantAction:  OperationActionRetry,
			wantUpdates: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeLambda{}
			executed := false
			var gotErr error
			resp := invokeStep(t, fake,
				stepPayload(`""`, checkpointedStep("1", "STARTED", nil)),
				func(ctx Context, _ string) (string, error) {
					out, err := Step(ctx, "s", func(StepContext) (string, error) {
						executed = true
						return "", nil
					}, WithSemantics(AtMostOncePerRetry), WithRetry(tt.retry))
					gotErr = err
					return out, err
				})

			if executed {
				t.Error("step body re-executed under at-most-once semantics")
			}
			if !strings.Contains(resp, tt.wantStatus) {
				t.Errorf("response = %s, want containing %s", resp, tt.wantStatus)
			}
			updates := updateBatch(t, fake)
			if len(updates) != tt.wantUpdates {
				t.Fatalf("received %d updates, want %d", len(updates), tt.wantUpdates)
			}
			assertStepUpdate(t, updates[0], "1", tt.wantAction)
			if tt.wantAction == OperationActionFail {
				// The interruption is recorded as the step's final error;
				// the StepError names it and rebuilds it as the cause.
				var stepErr *StepError
				if !errors.As(gotErr, &stepErr) || stepErr.ErrorType != "StepInterruptedError" {
					t.Errorf("step error = %v, want *StepError with ErrorType StepInterruptedError", gotErr)
				}
				var interrupted *StepInterruptedError
				if !errors.As(gotErr, &interrupted) {
					t.Errorf("step error = %v, want wrapping *StepInterruptedError", gotErr)
				}
			}
		})
	}
}

// upperSerdes uppercases serialized output; deserialization is standard.
type upperSerdes struct{}

func (upperSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	return []byte(strings.ToUpper(string(b))), err
}

func (upperSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func TestStepCustomSerdesRoundTrip(t *testing.T) {
	// The step's return value is the deserialized checkpointed payload,
	// so a transforming serdes is observable on first execution exactly
	// as it will be on replay.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`"hello world"`), func(ctx Context, event string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			return event, nil
		}, WithStepSerdes(upperSerdes{}))
	})

	if want := `{"Status":"SUCCEEDED","Result":"\"HELLO WORLD\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	updates := updateBatch(t, fake)
	if got := aws.ToString(updates[1].Payload); got != `"HELLO WORLD"` {
		t.Errorf("SUCCEED Payload = %q, want %q", got, `"HELLO WORLD"`)
	}
}

func TestStepNilResult(t *testing.T) {
	fake := &fakeLambda{}
	h := Wrap(func(ctx Context, _ string) (*string, error) {
		return Step(ctx, "s", func(StepContext) (*string, error) {
			return nil, nil
		})
	}, withLambdaAPI(fake))
	got, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if want := `{"Status":"SUCCEEDED","Result":"null"}`; string(got) != want {
		t.Errorf("response = %s, want %s", got, want)
	}
	updates := updateBatch(t, fake)
	if got := aws.ToString(updates[1].Payload); got != "null" {
		t.Errorf("SUCCEED Payload = %q, want null", got)
	}
}

func TestStepPanicIsFailedAttempt(t *testing.T) {
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			panic("step exploded")
		}, WithRetry(NoRetry()))
	})

	if !strings.Contains(resp, `"Status":"FAILED"`) {
		t.Errorf("response = %s, want FAILED", resp)
	}
	updates := updateBatch(t, fake)
	if len(updates) != 2 {
		t.Fatalf("received %d updates, want 2", len(updates))
	}
	if got := aws.ToString(updates[1].Error.ErrorMessage); !strings.Contains(got, "step exploded") {
		t.Errorf("FAIL ErrorMessage = %q, want containing panic value", got)
	}
}

func TestStepCheckpointErrorPropagates(t *testing.T) {
	wantErr := errors.New("throttled")
	fake := &fakeLambda{checkpointErr: wantErr}
	var gotErr error
	h := Wrap(func(ctx Context, _ string) (string, error) {
		out, err := Step(ctx, "s", func(StepContext) (string, error) { return "x", nil })
		gotErr = err
		return out, err
	}, withLambdaAPI(fake))
	_, invokeErr := h(context.Background(), stepPayload(`""`))

	if !errors.Is(gotErr, wantErr) {
		t.Errorf("Step() error = %v, want wrapping %v", gotErr, wantErr)
	}
	// An unstructured checkpoint failure is invocation-scoped, so passing
	// it through ends the invocation with an error rather than a FAILED
	// response.
	if !errors.Is(invokeErr, wantErr) {
		t.Errorf("Invoke() error = %v, want wrapping %v", invokeErr, wantErr)
	}
}

type transientError struct{ msg string }

func (e *transientError) Error() string { return e.msg }

func TestWireErrorTypeUserErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"errors.New", errors.New("x"), "Error"},
		{"fmt.Errorf", fmt.Errorf("x"), "Error"},
		{"wrapped", fmt.Errorf("x: %w", errors.New("y")), "Error"},
		{"joined", errors.Join(errors.New("x"), errors.New("y")), "Error"},
		{"custom type", &transientError{"x"}, "transientError"},
		{"interrupted", &StepInterruptedError{Name: "s"}, "StepInterruptedError"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := wireErrorType(tt.err); got != tt.want {
				t.Errorf("wireErrorType(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestStepCustomErrorTypeInCheckpoint(t *testing.T) {
	// A custom error type's name is recorded as the wire ErrorType, so
	// retry strategies keyed on error identity see a stable name.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		return Step(ctx, "s", func(StepContext) (string, error) {
			return "", &transientError{"flaky"}
		}, WithRetry(NoRetry()))
	})

	updates := updateBatch(t, fake)
	if got := aws.ToString(updates[1].Error.ErrorType); got != "transientError" {
		t.Errorf("ErrorType = %q, want transientError", got)
	}
}
