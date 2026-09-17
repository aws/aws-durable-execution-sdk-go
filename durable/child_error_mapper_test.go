package durable

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// paymentError is a handler-defined error type a mapper produces from a
// child context failure.
type paymentError struct {
	Reason string
}

func (e *paymentError) Error() string { return "payment failed: " + e.Reason }

// paymentMapper is an ordinary deterministic mapper: it recognizes only the
// error types that can escape the child body and returns anything else
// unchanged. It has no knowledge of its own output type.
func paymentMapper(err *ChildContextError) error {
	switch err.ErrorType {
	case "Error", "StepError":
		return &paymentError{Reason: err.Message}
	}
	return err
}

// childFailUpdate returns the FAIL update recorded for a child context.
func childFailUpdate(t *testing.T, fake *fakeLambda) OperationUpdate {
	t.Helper()
	for _, u := range updateBatch(t, fake) {
		if u.Action == OperationActionFail && u.Type == OperationTypeContext {
			return u
		}
	}
	t.Fatal("no FAIL checkpoint for the child context")
	return OperationUpdate{}
}

// replayOfFailure builds a replay payload whose child context is FAILED
// with the record a first invocation checkpointed.
func replayOfFailure(u OperationUpdate) []byte {
	return childPayload(`"x"`,
		checkpointedChild("1", "FAILED", &wireContextDetails{
			Error: &wireFullError{
				ErrorType:    aws.ToString(u.Error.ErrorType),
				ErrorMessage: aws.ToString(u.Error.ErrorMessage),
				ErrorData:    aws.ToString(u.Error.ErrorData),
				StackTrace:   u.Error.StackTrace,
			},
		}),
	)
}

func TestChildErrorMapperLive(t *testing.T) {
	// A mapper receives the *ChildContextError the SDK would otherwise
	// return, and its result is returned instead. The checkpoint records
	// the failure that escaped the body: the mapper's input.
	fake := &fakeLambda{}
	var received *ChildContextError
	calls := 0
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "pay", func(Context) (string, error) {
			return "", WithErrorData(errors.New("declined"), "d")
		}, WithChildErrorMapper(func(err *ChildContextError) error {
			calls++
			received = err
			return paymentMapper(err)
		}))
		var pe *paymentError
		if !errors.As(err, &pe) {
			t.Fatalf("err = %T (%v), want *paymentError", err, err)
		}
		return pe.Reason, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"declined\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if calls != 1 {
		t.Errorf("mapper called %d times, want 1", calls)
	}
	if received == nil || received.Name != "pay" || received.ErrorType != "Error" || received.Message != "declined" || received.ErrorData != "d" {
		t.Errorf("mapper input = %+v, want Name pay, ErrorType Error, Message declined, ErrorData d", received)
	}

	u := childFailUpdate(t, fake)
	if got := aws.ToString(u.Error.ErrorType); got != "Error" {
		t.Errorf("recorded ErrorType = %q, want Error", got)
	}
	if got := aws.ToString(u.Error.ErrorMessage); got != "declined" {
		t.Errorf("recorded ErrorMessage = %q, want declined", got)
	}
	if got := aws.ToString(u.Error.ErrorData); got != "d" {
		t.Errorf("recorded ErrorData = %q, want d", got)
	}
	if len(u.Error.StackTrace) == 0 {
		t.Error("recorded StackTrace is empty, want the body failure's trace")
	}
}

func TestChildErrorMapperReplay(t *testing.T) {
	// On replay the body does not run. The recorded failure is rebuilt as
	// a *ChildContextError and mapped again. The mapper handles only the
	// original ErrorType, and still reproduces the mapped type on replay,
	// because the record holds the original failure.
	fake := &fakeLambda{}
	payload := childPayload(`"x"`,
		checkpointedChild("1", "FAILED", &wireContextDetails{
			Error: &wireFullError{ErrorType: "Error", ErrorMessage: "declined", ErrorData: "d"},
		}),
	)
	var received *ChildContextError
	resp := invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "pay", func(Context) (string, error) {
			t.Fatal("fn should not execute on replay")
			return "", nil
		}, WithChildErrorMapper(func(err *ChildContextError) error {
			received = err
			return paymentMapper(err)
		}))
		var pe *paymentError
		if !errors.As(err, &pe) {
			t.Fatalf("err = %T (%v), want *paymentError", err, err)
		}
		return pe.Reason, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"declined\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	if received == nil || received.Name != "pay" || received.ErrorType != "Error" || received.Message != "declined" || received.ErrorData != "d" {
		t.Errorf("mapper input on replay = %+v, want Name pay, ErrorType Error, Message declined, ErrorData d", received)
	}
	if len(fake.gotUpdateBatches) != 0 {
		t.Errorf("replay issued %d checkpoint batches, want 0", len(fake.gotUpdateBatches))
	}
}

func TestChildErrorMapperInputIsTheSameLiveAndOnReplay(t *testing.T) {
	// The *ChildContextError the mapper receives on the first invocation
	// is built from the record the checkpoint stores. Replaying that
	// record rebuilds an equal *ChildContextError, so a deterministic
	// mapper produces the same result on both paths. The body fails with
	// a StepError so the record carries an SDK type, a trace, and data.
	live := &fakeLambda{}
	var liveInput *ChildContextError
	invokeStep(t, live, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "pay", func(c Context) (string, error) {
			return Step(c, "charge", func(StepContext) (string, error) {
				return "", WithErrorData(errors.New("declined"), "d")
			}, WithRetry(NoRetry()))
		}, WithChildErrorMapper(func(err *ChildContextError) error {
			liveInput = err
			return paymentMapper(err)
		}))
		var pe *paymentError
		if !errors.As(err, &pe) {
			t.Fatalf("live err = %T (%v), want *paymentError", err, err)
		}
		return pe.Reason, nil
	})
	if liveInput == nil {
		t.Fatal("mapper not called on the first invocation")
	}
	if liveInput.ErrorType != "StepError" || len(liveInput.StackTrace) == 0 {
		t.Fatalf("live input = %+v, want ErrorType StepError with a stack trace", liveInput)
	}

	replay := &fakeLambda{}
	var replayInput *ChildContextError
	invokeStep(t, replay, replayOfFailure(childFailUpdate(t, live)), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "pay", func(Context) (string, error) {
			t.Fatal("fn should not execute on replay")
			return "", nil
		}, WithChildErrorMapper(func(err *ChildContextError) error {
			replayInput = err
			return paymentMapper(err)
		}))
		var pe *paymentError
		if !errors.As(err, &pe) {
			t.Fatalf("replay err = %T (%v), want *paymentError", err, err)
		}
		return pe.Reason, nil
	})
	if replayInput == nil {
		t.Fatal("mapper not called on replay")
	}

	type fields struct {
		Name, ErrorType, Message, ErrorData string
		StackTrace                          []string
		Err                                 string
	}
	of := func(e *ChildContextError) fields {
		return fields{e.Name, e.ErrorType, e.Message, e.ErrorData, e.StackTrace, e.Err.Error()}
	}
	if got, want := of(replayInput), of(liveInput); !reflect.DeepEqual(got, want) {
		t.Errorf("mapper input on replay = %+v, want the first invocation's %+v", got, want)
	}
	var liveStep, replayStep *StepError
	if !errors.As(liveInput, &liveStep) || !errors.As(replayInput, &replayStep) {
		t.Errorf("errors.As(*StepError): live %v, replay %v; want both true", liveStep != nil, replayStep != nil)
	}
}

func TestChildErrorMapperAsyncLiveAndReplay(t *testing.T) {
	// The same mapping applies to RunInChildContextAsync and Go.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		fut := Go(ctx, "pay", func(Context) (string, error) {
			return "", errors.New("declined")
		}, WithChildErrorMapper(paymentMapper))
		_, err := fut.Result()
		var pe *paymentError
		if !errors.As(err, &pe) {
			t.Fatalf("live err = %T (%v), want *paymentError", err, err)
		}
		return pe.Reason, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"declined\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	u := childFailUpdate(t, fake)
	if got := aws.ToString(u.Error.ErrorType); got != "Error" {
		t.Errorf("recorded ErrorType = %q, want Error", got)
	}

	replay := &fakeLambda{}
	resp = invokeStep(t, replay, replayOfFailure(u), func(ctx Context, _ string) (string, error) {
		fut := RunInChildContextAsync(ctx, "pay", func(Context) (string, error) {
			t.Fatal("fn should not execute on replay")
			return "", nil
		}, WithChildErrorMapper(paymentMapper))
		_, err := fut.Result()
		var pe *paymentError
		if !errors.As(err, &pe) {
			t.Fatalf("replay err = %T (%v), want *paymentError", err, err)
		}
		return pe.Reason, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"declined\""}`; resp != want {
		t.Errorf("replay response = %s, want %s", resp, want)
	}
}

func TestChildErrorMapperNilResultKeepsChildContextError(t *testing.T) {
	// A nil result from the mapper is ignored: the *ChildContextError is
	// returned as if no mapper were set, so a failure cannot become a
	// success.
	fake := &fakeLambda{}
	resp := invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "pay", func(Context) (string, error) {
			return "", errors.New("declined")
		}, WithChildErrorMapper(func(*ChildContextError) error { return nil }))
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			t.Fatalf("err = %T (%v), want *ChildContextError", err, err)
		}
		return childErr.ErrorType + ":" + childErr.Message, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"Error:declined\""}`; resp != want {
		t.Errorf("response = %s, want %s", resp, want)
	}
	u := childFailUpdate(t, fake)
	if got := aws.ToString(u.Error.ErrorType); got != "Error" {
		t.Errorf("recorded ErrorType = %q, want Error", got)
	}

	// The same on replay.
	replay := &fakeLambda{}
	resp = invokeStep(t, replay, replayOfFailure(u), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "pay", func(Context) (string, error) {
			t.Fatal("fn should not execute on replay")
			return "", nil
		}, WithChildErrorMapper(func(*ChildContextError) error { return nil }))
		var childErr *ChildContextError
		if !errors.As(err, &childErr) {
			t.Fatalf("replay err = %T (%v), want *ChildContextError", err, err)
		}
		return childErr.ErrorType + ":" + childErr.Message, nil
	})
	if want := `{"Status":"SUCCEEDED","Result":"\"Error:declined\""}`; resp != want {
		t.Errorf("replay response = %s, want %s", resp, want)
	}
}

func TestChildErrorMapperRecordIsTheMapperInput(t *testing.T) {
	// Whatever the mapper returns, the checkpoint records the failure that
	// escaped the child body. The body fails with a StepError, whose
	// recorded message is its Error() text.
	const stepMessage = `durable: step "charge" failed after 1 attempts: Error: declined`
	tests := []struct {
		name   string
		mapper func(*ChildContextError) error
	}{
		{
			name:   "identity",
			mapper: func(e *ChildContextError) error { return e },
		},
		{
			name: "a new ChildContextError with other fields",
			mapper: func(e *ChildContextError) error {
				return &ChildContextError{Name: e.Name, ErrorType: "Renamed", Message: "renamed", ErrorData: "r"}
			},
		},
		{
			name:   "an unnamed error",
			mapper: func(e *ChildContextError) error { return errors.New("wrapped: " + e.Message) },
		},
		{
			name:   "an SDK error",
			mapper: func(e *ChildContextError) error { return &InvokeError{Name: e.Name, Message: e.Message} },
		},
		{
			name:   "a handler type carrying its own error data",
			mapper: func(e *ChildContextError) error { return WithErrorData(&paymentError{Reason: e.Message}, "m") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeLambda{}
			invokeStep(t, fake, childPayload(`"x"`), func(ctx Context, _ string) (string, error) {
				_, err := RunInChildContext(ctx, "pay", func(c Context) (string, error) {
					return Step(c, "charge", func(StepContext) (string, error) {
						return "", WithErrorData(errors.New("declined"), "d")
					}, WithRetry(NoRetry()))
				}, WithChildErrorMapper(tt.mapper))
				if err == nil {
					t.Fatal("expected the child to fail")
				}
				return "handled", nil
			})
			u := childFailUpdate(t, fake)
			if got := aws.ToString(u.Error.ErrorType); got != "StepError" {
				t.Errorf("recorded ErrorType = %q, want StepError", got)
			}
			if got := aws.ToString(u.Error.ErrorMessage); got != stepMessage {
				t.Errorf("recorded ErrorMessage = %q, want %q", got, stepMessage)
			}
			if got := aws.ToString(u.Error.ErrorData); got != "d" {
				t.Errorf("recorded ErrorData = %q, want d", got)
			}
		})
	}
}
