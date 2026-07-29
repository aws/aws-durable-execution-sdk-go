package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// failingSerdes fails Marshal and/or Unmarshal with a fixed cause and
// otherwise round-trips through encoding/json.
type failingSerdes struct {
	failMarshal   bool
	failUnmarshal bool
	cause         error
}

func (s failingSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	if s.failMarshal {
		return nil, s.cause
	}
	return json.Marshal(v)
}

func (s failingSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	if s.failUnmarshal {
		return s.cause
	}
	return json.Unmarshal(data, v)
}

// assertSerdesError asserts that err matches *SerdesError with the given
// operation and direction and that the Unwrap chain reaches cause.
func assertSerdesError(t *testing.T, err error, operation, direction string, cause error) {
	t.Helper()
	var serdesErr *SerdesError
	if !errors.As(err, &serdesErr) {
		t.Fatalf("error = %v (%T), want *SerdesError in chain", err, err)
	}
	if serdesErr.Operation != operation {
		t.Errorf("SerdesError.Operation = %q, want %q", serdesErr.Operation, operation)
	}
	if serdesErr.Direction != direction {
		t.Errorf("SerdesError.Direction = %q, want %q", serdesErr.Direction, direction)
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(err, cause) = false; Unwrap chain from %v does not reach %v", err, cause)
	}
}

func TestStepSerdesErrorLiveMarshal(t *testing.T) {
	// A step result that cannot be serialized fails the attempt with a
	// SerdesError. With retries exhausted, the step fails and the FAIL
	// checkpoint records SerdesError as the error type.
	cause := errors.New("marshal exploded")
	fake := &fakeLambda{}
	var got error
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "encode", func(StepContext) (string, error) {
			return "value", nil
		}, WithStepSerdes(failingSerdes{failMarshal: true, cause: cause}), WithRetry(NoRetry()))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "encode", "marshal", cause)
	var stepErr *StepError
	if !errors.As(got, &stepErr) {
		t.Fatalf("error = %v (%T), want *StepError wrapping the SerdesError", got, got)
	}

	var failUpdate *types.OperationUpdate
	for _, u := range updateBatch(t, fake) {
		if u.Action == types.OperationActionFail {
			failUpdate = &u
			break
		}
	}
	if failUpdate == nil {
		t.Fatal("no FAIL update checkpointed for the failed step")
	}
	if gotType := aws.ToString(failUpdate.Error.ErrorType); gotType != "SerdesError" {
		t.Errorf("FAIL update ErrorType = %q, want %q", gotType, "SerdesError")
	}
}

func TestStepSerdesErrorLiveUnmarshal(t *testing.T) {
	// The live round-trip deserialization (first-run value == replay
	// value) fails with a SerdesError.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	var got error
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "roundtrip", func(StepContext) (string, error) {
			return "value", nil
		}, WithStepSerdes(failingSerdes{failUnmarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "roundtrip", "unmarshal", cause)
}

func TestStepSerdesErrorReplayUnmarshal(t *testing.T) {
	// Replaying a SUCCEEDED step whose checkpointed result cannot be
	// deserialized fails with a SerdesError.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Result: `"value"`}))
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := Step(ctx, "replayed", func(StepContext) (string, error) {
			t.Error("step body must not re-execute on replay of a SUCCEEDED step")
			return "", nil
		}, WithStepSerdes(failingSerdes{failUnmarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "replayed", "unmarshal", cause)
}

func TestInvokeSerdesErrorPayloadMarshal(t *testing.T) {
	// An invoke payload that cannot be serialized fails with a
	// SerdesError before anything is checkpointed.
	cause := errors.New("marshal exploded")
	fake := &fakeLambda{}
	var got error
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Invoke[string](ctx, "call", "target", "input",
			WithInvokePayloadSerdes(failingSerdes{failMarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "call", "marshal", cause)
	if n := len(updateBatch(t, fake)); n != 0 {
		t.Errorf("invoke with unserializable payload sent %d updates, want 0", n)
	}
}

func TestInvokeSerdesErrorResultUnmarshal(t *testing.T) {
	// Replaying a SUCCEEDED invoke whose result cannot be deserialized
	// fails with a SerdesError.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	payload := stepPayload(`""`, wireOperation{
		Id:                   hashID("1"),
		Status:               "SUCCEEDED",
		ChainedInvokeDetails: &wireChainedInvokeDetails{Result: `"echoed"`},
	})
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := Invoke[string](ctx, "call", "target", "input",
			WithInvokeResultSerdes(failingSerdes{failUnmarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "call", "unmarshal", cause)
}

func TestCallbackSerdesErrorReplayUnmarshal(t *testing.T) {
	// A SUCCEEDED callback whose external payload cannot be deserialized
	// settles the callback's future with a SerdesError.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	payload := callbackPayload(`""`,
		checkpointedCallback("1", "SUCCEEDED", &wireCallbackDetails{
			CallbackId: "cb-1",
			Result:     `"external"`,
		}))
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		cb, err := CreateCallback[string](ctx, "confirm",
			WithCallbackSerdes(failingSerdes{failUnmarshal: true, cause: cause}))
		if err != nil {
			return "", err
		}
		_, err = cb.Result()
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "confirm", "unmarshal", cause)
}

func TestWaitForConditionSerdesErrorStateMarshal(t *testing.T) {
	// A wait-for-condition state that cannot be serialized fails with a
	// SerdesError.
	cause := errors.New("marshal exploded")
	fake := &fakeLambda{}
	var got error
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "poll", func(_ StepContext, state int) (int, error) {
			return state + 1, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			Serdes:       failingSerdes{failMarshal: true, cause: cause},
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "poll", "marshal", cause)
}

func TestWaitForConditionSerdesErrorTerminalUnmarshal(t *testing.T) {
	// Replaying a SUCCEEDED wait-for-condition whose final state cannot
	// be deserialized fails with a SerdesError.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Result: `5`}))
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := WaitForCondition(ctx, "poll", func(_ StepContext, state int) (int, error) {
			t.Error("check must not re-execute on replay of a SUCCEEDED operation")
			return state, nil
		}, ConditionConfig[int]{
			InitialState: 0,
			Serdes:       failingSerdes{failUnmarshal: true, cause: cause},
			WaitStrategy: func(_ int, _ int) WaitDecision {
				return WaitDecision{Continue: false}
			},
		})
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "poll", "unmarshal", cause)
}

func TestBatchItemSerdesErrorLiveMarshal(t *testing.T) {
	// A batch item result that cannot be serialized fails the batch with
	// a SerdesError naming the item.
	cause := errors.New("marshal exploded")
	fake := &fakeLambda{}
	var got error
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := Map(ctx, "fanout", []string{"a"},
			func(_ Context, item string, _ int) (string, error) {
				return item, nil
			},
			WithMaxConcurrency(1),
			WithBatchSerdes(failingSerdes{failMarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "item 0", "marshal", cause)
}

func TestBatchItemSerdesErrorReplayUnmarshal(t *testing.T) {
	// Replaying a SUCCEEDED batch whose checkpointed item result cannot
	// be deserialized fails with a SerdesError naming the item.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	aggregate := `{"results":[{"index":0,"status":1,"result":"\"a\""}],"reason":1}`
	payload := stepPayload(`""`, wireOperation{
		Id:             hashID("1"),
		Status:         "SUCCEEDED",
		ContextDetails: &wireContextDetails{Result: aggregate},
	})
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := Map(ctx, "fanout", []string{"a"},
			func(_ Context, item string, _ int) (string, error) {
				t.Error("item body must not re-execute on aggregate replay")
				return item, nil
			},
			WithMaxConcurrency(1),
			WithBatchSerdes(failingSerdes{failUnmarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "item 0", "unmarshal", cause)
}

func TestChildContextSerdesErrorReplayUnmarshal(t *testing.T) {
	// Replaying a SUCCEEDED child context whose checkpointed result
	// cannot be deserialized fails with a SerdesError.
	cause := errors.New("unmarshal exploded")
	fake := &fakeLambda{}
	payload := stepPayload(`""`,
		checkpointedChild("1", "SUCCEEDED", &wireContextDetails{Result: `"done"`}))
	var got error
	invokeStep(t, fake, payload, func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "sub", func(Context) (string, error) {
			t.Error("child body must not re-execute on replay of a SUCCEEDED context")
			return "", nil
		}, WithChildSerdes(failingSerdes{failUnmarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "sub", "unmarshal", cause)
}

func TestChildContextSerdesErrorLiveMarshal(t *testing.T) {
	// A child context result that cannot be serialized fails with a
	// SerdesError.
	cause := errors.New("marshal exploded")
	fake := &fakeLambda{}
	var got error
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		_, err := RunInChildContext(ctx, "sub", func(Context) (string, error) {
			return "done", nil
		}, WithChildSerdes(failingSerdes{failMarshal: true, cause: cause}))
		got = err
		return "", nil
	})

	assertSerdesError(t, got, "sub", "marshal", cause)
}
