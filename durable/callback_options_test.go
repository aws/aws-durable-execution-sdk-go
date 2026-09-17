package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Compile-time contract: every callback-level option is a CallbackOption
// and therefore also a WaitForCallbackOption. WithSubmitterRetry is a
// WaitForCallbackOption only, so it cannot be passed to CreateCallback.
var (
	_ CallbackOption        = WithCallbackTimeout(time.Second)
	_ CallbackOption        = WithCallbackHeartbeatTimeout(time.Second)
	_ CallbackOption        = WithCallbackSerdes(jsonSerdes{})
	_ WaitForCallbackOption = WithCallbackTimeout(time.Second)
	_ WaitForCallbackOption = WithCallbackHeartbeatTimeout(time.Second)
	_ WaitForCallbackOption = WithCallbackSerdes(jsonSerdes{})
	_ WaitForCallbackOption = WithSubmitterRetry(NoRetry())
)

func TestWithSubmitterRetryIsNotACallbackOption(t *testing.T) {
	// WithSubmitterRetry configures the submitter step, which only
	// WaitForCallback has. Its return type must not satisfy CallbackOption,
	// so CreateCallback(ctx, name, WithSubmitterRetry(...)) does not
	// compile and the option can never be silently ignored.
	var opt any = WithSubmitterRetry(NoRetry())
	if _, ok := opt.(CallbackOption); ok {
		t.Fatal("WithSubmitterRetry satisfies CallbackOption; CreateCallback would accept and ignore it")
	}
	if _, ok := opt.(WaitForCallbackOption); !ok {
		t.Fatal("WithSubmitterRetry does not satisfy WaitForCallbackOption")
	}
}

func TestCallbackOptionsApplyToBothOperations(t *testing.T) {
	// Every CallbackOption sets the same field whether it is applied
	// through the CreateCallback path or the WaitForCallback path.
	serdes := &testSerdes{suffix: "-x"}
	retry := NoRetry()
	forCreate := callbackOptions{}
	forWait := callbackOptions{}
	for _, o := range []CallbackOption{
		WithCallbackTimeout(3 * time.Second),
		WithCallbackHeartbeatTimeout(2 * time.Second),
		WithCallbackSerdes(serdes),
	} {
		o.applyCallback(&forCreate)
		o.applyWaitForCallback(&forWait)
	}
	WithSubmitterRetry(retry).applyWaitForCallback(&forWait)

	for name, got := range map[string]callbackOptions{"CreateCallback": forCreate, "WaitForCallback": forWait} {
		if got.timeout != 3*time.Second {
			t.Errorf("%s: timeout = %v, want 3s", name, got.timeout)
		}
		if got.heartbeatTimeout != 2*time.Second {
			t.Errorf("%s: heartbeatTimeout = %v, want 2s", name, got.heartbeatTimeout)
		}
		if got.serdes != serdes {
			t.Errorf("%s: serdes not applied", name)
		}
	}
	if forWait.retryStrategy == nil {
		t.Error("WaitForCallback: retryStrategy not applied")
	}
}

// wfcbInFlightPayload builds the checkpoint state of a WaitForCallback whose
// context is still in flight: the inner callback has SUCCEEDED with the
// given payload and the submitter step has SUCCEEDED, so the next replay
// resolves the callback result and completes the context.
func wfcbInFlightPayload(callbackResult string) []byte {
	return callbackPayload(`"cb"`,
		wireOperation{
			Id:             hashID("1"),
			Name:           "cb",
			Type:           string(OperationTypeContext),
			SubType:        operationSubTypeWaitForCallback,
			Status:         "STARTED",
			ContextDetails: &wireContextDetails{},
		},
		wireOperation{
			Id:       hashID("1-1"),
			ParentId: hashID("1"),
			Type:     string(OperationTypeCallback),
			SubType:  operationSubTypeCallback,
			Status:   "SUCCEEDED",
			CallbackDetails: &wireCallbackDetails{
				CallbackId: "cb-1",
				Result:     callbackResult,
			},
		},
		wireOperation{
			Id:          hashID("1-2"),
			ParentId:    hashID("1"),
			Type:        string(OperationTypeStep),
			SubType:     operationSubTypeStep,
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `{}`},
		},
	)
}

func TestWaitForCallbackAppliesCallbackSerdes(t *testing.T) {
	// WithCallbackSerdes passed to WaitForCallback must reach the callback
	// the operation creates, so the submitted payload is decoded with it.
	fake := &fakeLambda{statePages: [][]Operation{{}}}
	payload := wfcbInFlightPayload(`"hello"`)

	handler := func(ctx Context, event string) (string, error) {
		return WaitForCallback[string](ctx, event, func(StepContext, string) error {
			return nil
		}, WithCallbackSerdes(&testSerdes{suffix: "-custom"}))
	}

	h := Wrap(handler, withLambdaAPI(fake))
	got, err := h(context.Background(), payload)
	if err != nil {
		t.Fatalf("Invoke error: %v", err)
	}
	var resp invocationResponse
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Status != invocationSucceeded {
		t.Fatalf("response status = %q, want SUCCEEDED (%s)", resp.Status, got)
	}
	if got, want := aws.ToString(resp.Result), `"hello-custom"`; got != want {
		t.Errorf("result = %q, want %q", got, want)
	}
}

// TestCreateCallbackDefaultDeserialization pins the default callback
// deserializer: without WithCallbackSerdes or WithCallbackDeserializer, the
// submitted payload is decoded with the handler-level Serdes, which is
// encoding/json. A JSON string payload decodes into a Go string without its
// quotes, a numeric payload decodes into an int, and a bare (non-JSON) string
// payload is a deserialization error rather than a passthrough.
func TestCreateCallbackDefaultDeserialization(t *testing.T) {
	invoke := func(t *testing.T, payload string, handler Handler[string, string]) invocationResponse {
		t.Helper()
		fake := &fakeLambda{statePages: [][]Operation{{}}}
		in := callbackPayload(`"cb"`,
			checkpointedCallback("1", "SUCCEEDED", &wireCallbackDetails{
				CallbackId: "cb-1",
				Result:     payload,
			}),
		)
		h := Wrap(handler, withLambdaAPI(fake))
		got, err := h(context.Background(), in)
		if err != nil {
			t.Fatalf("Invoke error: %v", err)
		}
		var resp invocationResponse
		if err := json.Unmarshal(got, &resp); err != nil {
			t.Fatalf("unmarshal response: %v", err)
		}
		return resp
	}

	t.Run("json string payload decodes into string", func(t *testing.T) {
		resp := invoke(t, `"hello"`, func(ctx Context, event string) (string, error) {
			cb, err := CreateCallback[string](ctx, event)
			if err != nil {
				return "", err
			}
			return cb.Result()
		})
		if resp.Status != invocationSucceeded {
			t.Fatalf("status = %q, want SUCCEEDED", resp.Status)
		}
		if got, want := aws.ToString(resp.Result), `"hello"`; got != want {
			t.Errorf("result = %q, want %q", got, want)
		}
	})

	t.Run("numeric payload decodes into int", func(t *testing.T) {
		var seen int
		resp := invoke(t, `42`, func(ctx Context, event string) (string, error) {
			cb, err := CreateCallback[int](ctx, event)
			if err != nil {
				return "", err
			}
			n, err := cb.Result()
			if err != nil {
				return "", err
			}
			seen = n
			return "ok", nil
		})
		if resp.Status != invocationSucceeded {
			t.Fatalf("status = %q, want SUCCEEDED", resp.Status)
		}
		if seen != 42 {
			t.Errorf("callback result = %d, want 42", seen)
		}
	})

	t.Run("bare string payload is not passed through", func(t *testing.T) {
		var resultErr error
		resp := invoke(t, `hello`, func(ctx Context, event string) (string, error) {
			cb, err := CreateCallback[string](ctx, event)
			if err != nil {
				return "", err
			}
			_, resultErr = cb.Result()
			return "", resultErr
		})
		if resp.Status != invocationFailed {
			t.Fatalf("status = %q, want FAILED", resp.Status)
		}
		var serdesErr *SerdesError
		if !errors.As(resultErr, &serdesErr) {
			t.Fatalf("Result() error = %T (%v), want *SerdesError", resultErr, resultErr)
		}
		if serdesErr.Direction != serdesDirectionUnmarshal {
			t.Errorf("Direction = %q, want %q", serdesErr.Direction, serdesDirectionUnmarshal)
		}
	})
}
