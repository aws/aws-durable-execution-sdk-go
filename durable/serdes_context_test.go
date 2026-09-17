package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

// contextKey is an unexported key for test context values.
type contextKey struct{ name string }

// ctxRecordingSerdes captures the context.Context passed to Marshal and
// Unmarshal, allowing tests to assert that the invocation context (not a
// stale or synthetic background context) reaches the Serdes implementation.
type ctxRecordingSerdes struct {
	mu             sync.Mutex
	marshalCtxs    []context.Context
	unmarshalCtxs  []context.Context
	marshalMetas   []SerdesContext
	unmarshalMetas []SerdesContext
}

func (s *ctxRecordingSerdes) Marshal(ctx context.Context, meta SerdesContext, v any) ([]byte, error) {
	s.mu.Lock()
	s.marshalCtxs = append(s.marshalCtxs, ctx)
	s.marshalMetas = append(s.marshalMetas, meta)
	s.mu.Unlock()
	return json.Marshal(v)
}

func (s *ctxRecordingSerdes) Unmarshal(ctx context.Context, meta SerdesContext, data []byte, v any) error {
	s.mu.Lock()
	s.unmarshalCtxs = append(s.unmarshalCtxs, ctx)
	s.unmarshalMetas = append(s.unmarshalMetas, meta)
	s.mu.Unlock()
	return json.Unmarshal(data, v)
}

func TestSerdesReceivesInvocationContext(t *testing.T) {
	// Verify that a custom Serdes receives the Lambda invocation context
	// (the context.Context passed to handler.Invoke), not a synthetic
	// context.Background(). We inject a context value and assert the
	// Serdes observes it on both marshal and unmarshal paths.

	key := contextKey{name: "test-request-id"}
	wantVal := "req-abc-123"

	recorder := &ctxRecordingSerdes{}

	handler := func(ctx Context, event string) (string, error) {
		result, err := Step(ctx, "greet", func(_ StepContext) (string, error) {
			return "hello-" + event, nil
		})
		if err != nil {
			return "", err
		}
		return result, nil
	}

	fake := &fakeLambda{nextToken: "tok-1"}
	h := Wrap(handler, withLambdaAPI(fake), WithSerdes(recorder))

	// Invoke with a value-bearing context.
	ctx := context.WithValue(context.Background(), key, wantVal)
	payload := stepPayload(`"world"`)

	_, err := h(ctx, payload)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	// Marshal must have been called (step result serialization).
	recorder.mu.Lock()
	marshalCount := len(recorder.marshalCtxs)
	unmarshalCount := len(recorder.unmarshalCtxs)
	recorder.mu.Unlock()

	if marshalCount == 0 {
		t.Fatal("Serdes.Marshal was never called")
	}

	// Every Marshal call must carry the invocation context value.
	for i, ctx := range recorder.marshalCtxs {
		got := ctx.Value(key)
		if got != wantVal {
			t.Errorf("Marshal[%d] context value = %v, want %q", i, got, wantVal)
		}
	}

	// On first invocation with no prior state, Unmarshal may not be
	// called (no replay). Re-invoke with the step already succeeded to
	// exercise the unmarshal (replay) path.
	fake.nextToken = "tok-2"
	replayPayload := stepPayload(`"world"`, checkpointedStep("1", "SUCCEEDED",
		&wireStepDetails{Result: `"hello-world"`}))

	ctx2 := context.WithValue(context.Background(), key, "req-replay-456")
	_, err = h(ctx2, replayPayload)
	if err != nil {
		t.Fatalf("Invoke (replay): %v", err)
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	if len(recorder.unmarshalCtxs) == 0 {
		t.Fatal("Serdes.Unmarshal was never called on replay")
	}

	// Unmarshal calls during replay must carry the replay invocation's context.
	for i := unmarshalCount; i < len(recorder.unmarshalCtxs); i++ {
		got := recorder.unmarshalCtxs[i].Value(key)
		if got != "req-replay-456" {
			t.Errorf("Unmarshal[%d] context value = %v, want %q", i, got, "req-replay-456")
		}
	}
}
