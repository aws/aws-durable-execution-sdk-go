package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
			SubType:        OperationSubTypeWaitForCallback,
			Status:         "STARTED",
			ContextDetails: &wireContextDetails{},
		},
		wireOperation{
			Id:       hashID("1-1"),
			ParentId: hashID("1"),
			Type:     string(OperationTypeCallback),
			SubType:  OperationSubTypeCallback,
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
			SubType:     OperationSubTypeStep,
			Status:      "SUCCEEDED",
			StepDetails: &wireStepDetails{Result: `{}`},
		},
	)
}

func TestWaitForCallbackAppliesCallbackSerdes(t *testing.T) {
	// WithCallbackSerdes passed to WaitForCallback decodes the submitted
	// payload into the returned value.
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
// submitted payload is returned unchanged by RawSerdes. A JSON string
// payload keeps its quotes, a numeric payload read into a string is the
// digits, and a non-JSON payload is passed through. A result type RawSerdes
// does not support fails with a *SerdesError that names the type.
// WithCallbackSerdes(JSONSerdes) on the operation and WithCallbackDeserializer
// on the handler each select JSON decoding.
func TestCreateCallbackDefaultDeserialization(t *testing.T) {
	invoke := func(t *testing.T, payload string, handler Handler[string, string], hopts ...HandlerOption) invocationResponse {
		t.Helper()
		fake := &fakeLambda{statePages: [][]Operation{{}}}
		in := callbackPayload(`"cb"`,
			checkpointedCallback("1", "SUCCEEDED", &wireCallbackDetails{
				CallbackId: "cb-1",
				Result:     payload,
			}),
		)
		h := Wrap(handler, append([]HandlerOption{withLambdaAPI(fake)}, hopts...)...)
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

	stringCallback := func(opts ...CallbackOption) Handler[string, string] {
		return func(ctx Context, event string) (string, error) {
			cb, err := CreateCallback[string](ctx, event, opts...)
			if err != nil {
				return "", err
			}
			return cb.Result(ctx)
		}
	}
	// wantResult checks the handler's returned string, which the
	// invocation response stores as JSON.
	wantResult := func(t *testing.T, resp invocationResponse, want string) {
		t.Helper()
		if resp.Status != invocationSucceeded {
			t.Fatalf("status = %q, want SUCCEEDED", resp.Status)
		}
		var got string
		if err := json.Unmarshal([]byte(aws.ToString(resp.Result)), &got); err != nil {
			t.Fatalf("decode result %q: %v", aws.ToString(resp.Result), err)
		}
		if got != want {
			t.Errorf("callback result = %q, want %q", got, want)
		}
	}

	t.Run("json string payload keeps its quotes", func(t *testing.T) {
		wantResult(t, invoke(t, `"hello"`, stringCallback()), `"hello"`)
	})

	t.Run("numeric payload into a string is the digits", func(t *testing.T) {
		wantResult(t, invoke(t, `42`, stringCallback()), `42`)
	})

	t.Run("bare string payload is passed through", func(t *testing.T) {
		wantResult(t, invoke(t, `hello`, stringCallback()), `hello`)
	})

	t.Run("byte slice and raw message results hold the payload", func(t *testing.T) {
		var gotBytes []byte
		var gotRaw json.RawMessage
		resp := invoke(t, `{"a":1}`, func(ctx Context, event string) (string, error) {
			b, err := CreateCallback[[]byte](ctx, event)
			if err != nil {
				return "", err
			}
			if gotBytes, err = b.Result(ctx); err != nil {
				return "", err
			}
			return "ok", nil
		})
		if resp.Status != invocationSucceeded {
			t.Fatalf("[]byte status = %q, want SUCCEEDED", resp.Status)
		}
		if string(gotBytes) != `{"a":1}` {
			t.Errorf("[]byte result = %q, want %q", gotBytes, `{"a":1}`)
		}
		resp = invoke(t, `{"a":1}`, func(ctx Context, event string) (string, error) {
			r, err := CreateCallback[json.RawMessage](ctx, event)
			if err != nil {
				return "", err
			}
			if gotRaw, err = r.Result(ctx); err != nil {
				return "", err
			}
			return "ok", nil
		})
		if resp.Status != invocationSucceeded {
			t.Fatalf("json.RawMessage status = %q, want SUCCEEDED", resp.Status)
		}
		if string(gotRaw) != `{"a":1}` {
			t.Errorf("json.RawMessage result = %q, want %q", gotRaw, `{"a":1}`)
		}
	})

	t.Run("unsupported result type fails with a SerdesError", func(t *testing.T) {
		var resultErr error
		resp := invoke(t, `42`, func(ctx Context, event string) (string, error) {
			cb, err := CreateCallback[int](ctx, event)
			if err != nil {
				return "", err
			}
			_, resultErr = cb.Result(ctx)
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
		if !strings.Contains(serdesErr.Error(), "*int") {
			t.Errorf("error %q does not name the type *int", serdesErr.Error())
		}
	})

	t.Run("WithCallbackSerdes(JSONSerdes) decodes the payload", func(t *testing.T) {
		wantResult(t, invoke(t, `"hello"`, stringCallback(WithCallbackSerdes(JSONSerdes))), `hello`)
	})

	t.Run("WithCallbackDeserializer decodes every payload", func(t *testing.T) {
		jsonDeser := DeserializerFunc(json.Unmarshal)
		wantResult(t, invoke(t, `"hello"`, stringCallback(), WithCallbackDeserializer(jsonDeser)), `hello`)
		var seen int
		resp := invoke(t, `42`, func(ctx Context, event string) (string, error) {
			cb, err := CreateCallback[int](ctx, event)
			if err != nil {
				return "", err
			}
			if seen, err = cb.Result(ctx); err != nil {
				return "", err
			}
			return "ok", nil
		}, WithCallbackDeserializer(jsonDeser))
		if resp.Status != invocationSucceeded {
			t.Fatalf("status = %q, want SUCCEEDED", resp.Status)
		}
		if seen != 42 {
			t.Errorf("callback result = %d, want 42", seen)
		}
	})
}

// TestWaitForCallbackEmptyResult pins that a callback completed with no
// payload makes WaitForCallback return the zero value of its result type,
// on the first run and on replay, without calling the result serdes.
// RawSerdes does not support an O of any, so a call would fail.
func TestWaitForCallbackEmptyResult(t *testing.T) {
	handler := func(ctx Context, event string) (any, error) {
		return WaitForCallback[any](ctx, event, func(StepContext, string) error {
			return nil
		})
	}
	payloads := map[string][]byte{
		"first run": wfcbInFlightPayload(""),
		"replay": callbackPayload(`"cb"`, wireOperation{
			Id:             hashID("1"),
			Name:           "cb",
			Type:           string(OperationTypeContext),
			SubType:        OperationSubTypeWaitForCallback,
			Status:         "SUCCEEDED",
			ContextDetails: &wireContextDetails{},
		}),
	}
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			fake := &fakeLambda{statePages: [][]Operation{{}}}
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
			if got, want := aws.ToString(resp.Result), `null`; got != want {
				t.Errorf("result = %q, want %q", got, want)
			}
		})
	}
}
