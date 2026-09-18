package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// upperDeserializer is a callback Deserializer that decodes a JSON string
// and upper-cases it, so a test can prove the configured deserializer ran.
type upperDeserializer struct{}

func (upperDeserializer) Unmarshal(data []byte, v any) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	p, ok := v.(*string)
	if !ok {
		return errors.New("upperDeserializer: target is not *string")
	}
	*p = strings.ToUpper(s)
	return nil
}

// succeededStepPayload returns the payload of the first SUCCEED step update
// the fake recorded.
func succeededStepPayload(t *testing.T, fake *fakeLambda) string {
	t.Helper()
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeStep && u.Action == OperationActionSucceed {
			return aws.ToString(u.Payload)
		}
	}
	t.Fatal("no SUCCEED step update checkpointed")
	return ""
}

func TestConfigureSerdesStepUsesNewSerdes(t *testing.T) {
	// A step started after ConfigureSerdes is checkpointed with the new
	// serdes, and the value returned to the handler is the decoded form of
	// that payload.
	fake := &fakeLambda{}
	var got receipt
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		if err := ConfigureSerdes(ctx, SerdesConfig{Serdes: maskedReceiptSerdes("RCPT:")}); err != nil {
			return "", err
		}
		r, err := Step(ctx, "charge", func(StepContext) (receipt, error) {
			return receipt{ID: "r1", Card: "4111111111111111"}, nil
		}, WithRetry(NoRetry()))
		if err != nil {
			return "", err
		}
		got = r
		return "", nil
	})

	if got != (receipt{ID: "r1", Card: "****1111"}) {
		t.Errorf("Step() = %+v, want the masked receipt", got)
	}
	if p := succeededStepPayload(t, fake); p != `RCPT:{"id":"r1","card":"****1111"}` {
		t.Errorf("checkpointed payload = %q, want the configured serdes encoding", p)
	}
}

func TestConfigureSerdesReplayReproducesResult(t *testing.T) {
	// On replay the handler runs the same ConfigureSerdes call before the
	// step, so the checkpointed payload decodes with the same serdes and
	// the step returns the same value it returned on the first invocation.
	// The step body does not run.
	fake := &fakeLambda{}
	var got receipt
	invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `RCPT:{"id":"r1","card":"****1111"}`})),
		func(ctx Context, _ string) (string, error) {
			if err := ConfigureSerdes(ctx, SerdesConfig{Serdes: maskedReceiptSerdes("RCPT:")}); err != nil {
				return "", err
			}
			r, err := Step(ctx, "charge", func(StepContext) (receipt, error) {
				t.Fatal("step body ran on replay")
				return receipt{}, nil
			}, WithRetry(NoRetry()))
			if err != nil {
				return "", err
			}
			got = r
			return "", nil
		})
	if got != (receipt{ID: "r1", Card: "****1111"}) {
		t.Errorf("replayed Step() = %+v, want the checkpointed receipt", got)
	}
}

func TestConfigureSerdesReplayWithDifferentSerdesFails(t *testing.T) {
	// The documented consequence of a non-deterministic configuration: the
	// checkpoint was written with one serdes and the replay configures
	// another, so the replay cannot decode the payload and the step fails
	// with a SerdesError.
	fake := &fakeLambda{}
	var gotErr error
	invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `RCPT:{"id":"r1","card":"****1111"}`})),
		func(ctx Context, _ string) (string, error) {
			if err := ConfigureSerdes(ctx, SerdesConfig{Serdes: maskedReceiptSerdes("OTHER:")}); err != nil {
				return "", err
			}
			_, gotErr = Step(ctx, "charge", func(StepContext) (receipt, error) {
				t.Fatal("step body ran on replay")
				return receipt{}, nil
			}, WithRetry(NoRetry()))
			return "", nil
		})
	var serdesErr *SerdesError
	if !errors.As(gotErr, &serdesErr) {
		t.Fatalf("Step() error = %v (%T), want *SerdesError", gotErr, gotErr)
	}
}

func TestConfigureSerdesPerOperationOptionWins(t *testing.T) {
	// A per-operation serdes option still takes precedence over the
	// configured default.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		if err := ConfigureSerdes(ctx, SerdesConfig{Serdes: maskedReceiptSerdes("DEFAULT:")}); err != nil {
			return "", err
		}
		_, err := Step(ctx, "charge", func(StepContext) (receipt, error) {
			return receipt{ID: "r1", Card: "4111111111111111"}, nil
		}, WithRetry(NoRetry()), WithStepSerdes(maskedReceiptSerdes("PEROP:")))
		return "", err
	})
	if p := succeededStepPayload(t, fake); !strings.HasPrefix(p, "PEROP:") {
		t.Errorf("checkpointed payload = %q, want the per-operation serdes encoding", p)
	}
}

func TestConfigureSerdesOperationBeforeCallKeepsOldSerdes(t *testing.T) {
	// A step started before ConfigureSerdes uses the previous default; a
	// step started after uses the new one.
	fake := &fakeLambda{}
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		if _, err := Step(ctx, "before", func(StepContext) (receipt, error) {
			return receipt{ID: "r1", Card: "4111111111111111"}, nil
		}, WithRetry(NoRetry())); err != nil {
			return "", err
		}
		if err := ConfigureSerdes(ctx, SerdesConfig{Serdes: maskedReceiptSerdes("RCPT:")}); err != nil {
			return "", err
		}
		_, err := Step(ctx, "after", func(StepContext) (receipt, error) {
			return receipt{ID: "r2", Card: "4111111111112222"}, nil
		}, WithRetry(NoRetry()))
		return "", err
	})

	payloads := map[string]string{}
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeStep && u.Action == OperationActionSucceed && u.Id != nil {
			payloads[*u.Id] = aws.ToString(u.Payload)
		}
	}
	if got := payloads[hashID("1")]; got != `{"id":"r1","card":"4111111111111111"}` {
		t.Errorf("step before ConfigureSerdes payload = %q, want plain JSON", got)
	}
	if got := payloads[hashID("2")]; got != `RCPT:{"id":"r2","card":"****2222"}` {
		t.Errorf("step after ConfigureSerdes payload = %q, want the configured encoding", got)
	}
}

func TestConfigureSerdesCallbackDeserializer(t *testing.T) {
	// A callback resolved after ConfigureSerdes decodes its payload with
	// the configured CallbackDeserializer, and a per-operation
	// WithCallbackSerdes still wins over it.
	run := func(t *testing.T, opts ...CallbackOption) string {
		t.Helper()
		fake := &fakeLambda{statePages: [][]Operation{{}}}
		in := callbackPayload(`"cb"`,
			checkpointedCallback("1", "SUCCEEDED", &wireCallbackDetails{CallbackId: "cb-1", Result: `"hello"`}),
		)
		var got string
		handler := func(ctx Context, event string) (string, error) {
			if err := ConfigureSerdes(ctx, SerdesConfig{CallbackDeserializer: upperDeserializer{}}); err != nil {
				return "", err
			}
			cb, err := CreateCallback[string](ctx, event, opts...)
			if err != nil {
				return "", err
			}
			got, err = cb.Result()
			return got, err
		}
		if _, err := Wrap(handler, withLambdaAPI(fake))(context.Background(), in); err != nil {
			t.Fatalf("Invoke() error: %v", err)
		}
		return got
	}

	if got := run(t); got != "HELLO" {
		t.Errorf("callback result = %q, want the configured deserializer's output %q", got, "HELLO")
	}
	if got := run(t, WithCallbackSerdes(JSONSerdes)); got != "hello" {
		t.Errorf("callback result with per-operation serdes = %q, want %q", got, "hello")
	}
}

func TestConfigureSerdesNilFieldKeepsCurrent(t *testing.T) {
	ec := newTestContext(t, []*operation{execOp()})
	initial := ec.serdesDefaults().serdes

	if err := ConfigureSerdes(ec, SerdesConfig{CallbackDeserializer: upperDeserializer{}}); err != nil {
		t.Fatalf("ConfigureSerdes() error: %v", err)
	}
	if ec.serdesDefaults().serdes != initial {
		t.Errorf("serdes changed to %T when only CallbackDeserializer was set", ec.serdesDefaults().serdes)
	}
	if _, ok := ec.serdesDefaults().callbackDeserializer.(upperDeserializer); !ok {
		t.Errorf("callbackDeserializer = %T, want upperDeserializer", ec.serdesDefaults().callbackDeserializer)
	}

	custom := &ctxRecordingSerdes{}
	if err := ConfigureSerdes(ec, SerdesConfig{Serdes: custom}); err != nil {
		t.Fatalf("ConfigureSerdes() error: %v", err)
	}
	if ec.serdesDefaults().serdes != custom {
		t.Errorf("serdes = %T, want the configured serdes", ec.serdesDefaults().serdes)
	}
	if _, ok := ec.serdesDefaults().callbackDeserializer.(upperDeserializer); !ok {
		t.Errorf("callbackDeserializer changed to %T when only Serdes was set", ec.serdesDefaults().callbackDeserializer)
	}

	if err := ConfigureSerdes(ec, SerdesConfig{}); err != nil {
		t.Fatalf("ConfigureSerdes(empty) error: %v", err)
	}
	if ec.serdesDefaults().serdes != custom {
		t.Errorf("serdes changed to %T on an empty config", ec.serdesDefaults().serdes)
	}
}

func TestConfigureSerdesPropagatesToContextsDerivedAfter(t *testing.T) {
	// A child or branch derived after the call inherits the new defaults;
	// one derived before keeps the defaults it was derived with.
	ec := newTestContext(t, []*operation{execOp()})
	before := ec.child("1", "", ec.owner, modeExecution)

	custom := &ctxRecordingSerdes{}
	if err := ConfigureSerdes(ec, SerdesConfig{Serdes: custom, CallbackDeserializer: upperDeserializer{}}); err != nil {
		t.Fatalf("ConfigureSerdes() error: %v", err)
	}
	after := ec.child("2", "", ec.owner, modeExecution)
	branch := ec.branch(ec.owner)

	if before.serdesDefaults().serdes != JSONSerdes || before.serdesDefaults().callbackDeserializer != nil {
		t.Errorf("child derived before the call: serdes = %T, callbackDeserializer = %T; want the original defaults", before.serdesDefaults().serdes, before.serdesDefaults().callbackDeserializer)
	}
	for name, c := range map[string]*execContext{"child": after, "branch": branch} {
		if c.serdesDefaults().serdes != custom {
			t.Errorf("%s derived after the call: serdes = %T, want the configured serdes", name, c.serdesDefaults().serdes)
		}
		if _, ok := c.serdesDefaults().callbackDeserializer.(upperDeserializer); !ok {
			t.Errorf("%s derived after the call: callbackDeserializer = %T, want upperDeserializer", name, c.serdesDefaults().callbackDeserializer)
		}
	}
}

func TestConfigureSerdesForeignContext(t *testing.T) {
	err := ConfigureSerdes(nil, SerdesConfig{Serdes: JSONSerdes})
	if err == nil || !strings.Contains(err.Error(), "Context was not created by the SDK") {
		t.Errorf("ConfigureSerdes(nil) error = %v, want a foreign context error", err)
	}
}

// stepPayloadsByID returns every SUCCEED step payload the fake recorded,
// keyed by the step's positional operation ID.
func stepPayloadsByID(t *testing.T, fake *fakeLambda) map[string]string {
	t.Helper()
	payloads := map[string]string{}
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeStep && u.Action == OperationActionSucceed && u.Id != nil {
			payloads[aws.ToString(u.Id)] = aws.ToString(u.Payload)
		}
	}
	return payloads
}

func TestConfigureSerdesAfterRunInChildContextAsync(t *testing.T) {
	// The owner launches a child asynchronously and then calls
	// ConfigureSerdes while the child goroutine may still be deriving its
	// context. The child keeps the defaults in effect when it was launched,
	// on every schedule, and the race detector sees no unsynchronized
	// access. A step started on the owner after the call uses the new
	// serdes.
	for range 20 {
		fake := &fakeLambda{}
		invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
			fut := RunInChildContextAsync(ctx, "child", func(c Context) (string, error) {
				_, err := Step(c, "inner", func(StepContext) (receipt, error) {
					return receipt{ID: "r1", Card: "4111111111111111"}, nil
				}, WithRetry(NoRetry()))
				return "", err
			})
			if err := ConfigureSerdes(ctx, SerdesConfig{Serdes: maskedReceiptSerdes("RCPT:")}); err != nil {
				return "", err
			}
			if _, err := fut.Result(); err != nil {
				return "", err
			}
			_, err := Step(ctx, "after", func(StepContext) (receipt, error) {
				return receipt{ID: "r2", Card: "4111111111112222"}, nil
			}, WithRetry(NoRetry()))
			return "", err
		})

		payloads := stepPayloadsByID(t, fake)
		if got := payloads[hashID("1-1")]; got != `{"id":"r1","card":"4111111111111111"}` {
			t.Fatalf("child step payload = %q, want the defaults in effect when the child was launched", got)
		}
		if got := payloads[hashID("2")]; got != `RCPT:{"id":"r2","card":"****2222"}` {
			t.Fatalf("step after ConfigureSerdes payload = %q, want the configured encoding", got)
		}
	}
}

func TestConfigureSerdesAfterStepAsync(t *testing.T) {
	// StepAsync followed by ConfigureSerdes: the step's branch context is
	// derived on the step goroutine while the owner writes the new
	// defaults. The step keeps the serdes it was launched with and no
	// unsynchronized access is reported.
	for range 20 {
		fake := &fakeLambda{}
		invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
			fut := StepAsync(ctx, "async", func(StepContext) (receipt, error) {
				return receipt{ID: "r1", Card: "4111111111111111"}, nil
			}, WithRetry(NoRetry()))
			if err := ConfigureSerdes(ctx, SerdesConfig{Serdes: maskedReceiptSerdes("RCPT:"), CallbackDeserializer: upperDeserializer{}}); err != nil {
				return "", err
			}
			_, err := fut.Result()
			return "", err
		})

		if got := stepPayloadsByID(t, fake)[hashID("1")]; got != `{"id":"r1","card":"4111111111111111"}` {
			t.Fatalf("async step payload = %q, want plain JSON", got)
		}
	}
}
