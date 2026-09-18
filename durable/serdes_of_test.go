package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// receipt is the single type the SerdesOf tests serialize.
type receipt struct {
	ID   string `json:"id"`
	Card string `json:"card"`
}

// maskedReceiptSerdes returns a SerdesOf serdes that masks the card number
// on marshal and prefixes the payload with marker so tests can prove the
// custom marshal ran.
func maskedReceiptSerdes(marker string) Serdes {
	return SerdesOf(
		func(_ context.Context, _ SerdesContext, r receipt) ([]byte, error) {
			r.Card = "****" + r.Card[len(r.Card)-4:]
			b, err := json.Marshal(r)
			if err != nil {
				return nil, err
			}
			return append([]byte(marker), b...), nil
		},
		func(_ context.Context, _ SerdesContext, b []byte) (receipt, error) {
			var r receipt
			if !strings.HasPrefix(string(b), marker) {
				return r, errors.New("missing marker")
			}
			return r, json.Unmarshal(b[len(marker):], &r)
		},
	)
}

func TestSerdesOfMarshalRejectsOtherType(t *testing.T) {
	s := maskedReceiptSerdes("RCPT:")
	_, err := s.Marshal(context.Background(), SerdesContext{}, "not a receipt")
	if err == nil {
		t.Fatal("Marshal(string) error = nil, want type mismatch")
	}
	for _, want := range []string{"durable.receipt", "string"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Marshal error = %q, want it to name %q", err, want)
		}
	}
}

func TestSerdesOfMarshalRejectsPointerToT(t *testing.T) {
	// *T is not T. The SDK always passes results by value, so a pointer
	// is a caller mistake and is reported as one.
	s := maskedReceiptSerdes("RCPT:")
	_, err := s.Marshal(context.Background(), SerdesContext{}, &receipt{ID: "r1", Card: "4111111111111111"})
	if err == nil {
		t.Fatal("Marshal(*receipt) error = nil, want type mismatch")
	}
	if !strings.Contains(err.Error(), "*durable.receipt") || !strings.Contains(err.Error(), "want durable.receipt") {
		t.Errorf("Marshal error = %q, want it to name *durable.receipt and durable.receipt", err)
	}
}

func TestSerdesOfUnmarshalRejectsOtherTarget(t *testing.T) {
	s := maskedReceiptSerdes("RCPT:")
	var target string
	err := s.Unmarshal(context.Background(), SerdesContext{}, []byte(`RCPT:{}`), &target)
	if err == nil {
		t.Fatal("Unmarshal(*string) error = nil, want type mismatch")
	}
	for _, want := range []string{"*durable.receipt", "*string"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Unmarshal error = %q, want it to name %q", err, want)
		}
	}

	// A T passed by value instead of *T is the same class of mistake.
	err = s.Unmarshal(context.Background(), SerdesContext{}, []byte(`RCPT:{}`), receipt{})
	if err == nil || !strings.Contains(err.Error(), "got durable.receipt") {
		t.Errorf("Unmarshal(receipt) error = %v, want type mismatch naming durable.receipt", err)
	}

	// A nil *T cannot be filled.
	var nilTarget *receipt
	err = s.Unmarshal(context.Background(), SerdesContext{}, []byte(`RCPT:{}`), nilTarget)
	if err == nil || !strings.Contains(err.Error(), "nil *durable.receipt") {
		t.Errorf("Unmarshal(nil *receipt) error = %v, want nil-pointer error", err)
	}
}

func TestSerdesOfDirectRoundTrip(t *testing.T) {
	s := maskedReceiptSerdes("RCPT:")
	in := receipt{ID: "r1", Card: "4111111111111111"}

	data, err := s.Marshal(context.Background(), SerdesContext{}, in)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if got := string(data); got != `RCPT:{"id":"r1","card":"****1111"}` {
		t.Errorf("Marshal() = %q, want masked, marker-prefixed JSON", got)
	}

	var out receipt
	if err := s.Unmarshal(context.Background(), SerdesContext{}, data, &out); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	if out != (receipt{ID: "r1", Card: "****1111"}) {
		t.Errorf("Unmarshal() = %+v, want masked receipt", out)
	}
}

func TestSerdesOfPropagatesFunctionErrors(t *testing.T) {
	marshalErr := errors.New("marshal failed")
	unmarshalErr := errors.New("unmarshal failed")
	s := SerdesOf(
		func(context.Context, SerdesContext, receipt) ([]byte, error) { return nil, marshalErr },
		func(context.Context, SerdesContext, []byte) (receipt, error) { return receipt{}, unmarshalErr },
	)
	if _, err := s.Marshal(context.Background(), SerdesContext{}, receipt{}); !errors.Is(err, marshalErr) {
		t.Errorf("Marshal error = %v, want %v", err, marshalErr)
	}
	var out receipt
	if err := s.Unmarshal(context.Background(), SerdesContext{}, nil, &out); !errors.Is(err, unmarshalErr) {
		t.Errorf("Unmarshal error = %v, want %v", err, unmarshalErr)
	}
}

func TestSerdesOfNilFunctions(t *testing.T) {
	s := SerdesOf[receipt](nil, nil)
	if _, err := s.Marshal(context.Background(), SerdesContext{}, receipt{}); err == nil || !strings.Contains(err.Error(), "marshal function is nil") {
		t.Errorf("Marshal error = %v, want nil-function error", err)
	}
	var out receipt
	if err := s.Unmarshal(context.Background(), SerdesContext{}, nil, &out); err == nil || !strings.Contains(err.Error(), "unmarshal function is nil") {
		t.Errorf("Unmarshal error = %v, want nil-function error", err)
	}
}

func TestSerdesOfWithStepSerdesRoundTrip(t *testing.T) {
	// A SerdesOf serdes attached to one step: the checkpointed payload
	// carries the custom encoding, and the value the step returns is the
	// deserialized form of that payload.
	fake := &fakeLambda{}
	var got receipt
	invokeStep(t, fake, stepPayload(`""`), func(ctx Context, _ string) (string, error) {
		r, err := Step(ctx, "charge", func(StepContext) (receipt, error) {
			return receipt{ID: "r1", Card: "4111111111111111"}, nil
		}, WithStepSerdes(maskedReceiptSerdes("RCPT:")))
		if err != nil {
			return "", err
		}
		got = r
		return "", nil
	})

	if got != (receipt{ID: "r1", Card: "****1111"}) {
		t.Errorf("Step() = %+v, want the deserialized (masked) receipt", got)
	}

	var succeed *OperationUpdate
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeStep && u.Action == OperationActionSucceed {
			succeed = &u
			break
		}
	}
	if succeed == nil {
		t.Fatal("no SUCCEED step update checkpointed")
	}
	if p := aws.ToString(succeed.Payload); p != `RCPT:{"id":"r1","card":"****1111"}` {
		t.Errorf("checkpointed payload = %q, want the SerdesOf encoding", p)
	}
}

func TestSerdesOfWithStepSerdesReplay(t *testing.T) {
	// On replay the step result is decoded from the checkpoint with the
	// SerdesOf unmarshal function.
	fake := &fakeLambda{}
	var got receipt
	invokeStep(t, fake,
		stepPayload(`""`, checkpointedStep("1", "SUCCEEDED", &wireStepDetails{Attempt: 1, Result: `RCPT:{"id":"r9","card":"****9999"}`})),
		func(ctx Context, _ string) (string, error) {
			r, err := Step(ctx, "charge", func(StepContext) (receipt, error) {
				t.Fatal("step body ran on replay")
				return receipt{}, nil
			}, WithStepSerdes(maskedReceiptSerdes("RCPT:")))
			if err != nil {
				return "", err
			}
			got = r
			return "", nil
		})
	if got != (receipt{ID: "r9", Card: "****9999"}) {
		t.Errorf("replayed Step() = %+v, want the checkpointed receipt", got)
	}
}

func TestSerdesOfHandlerWide(t *testing.T) {
	// A SerdesOf serdes set with WithSerdes serves every operation result.
	// A step returning T round-trips. A step returning another type fails
	// at runtime with a SerdesError whose message names both types; this
	// is the documented behaviour of a single-type serdes used
	// handler-wide.
	fake := &fakeLambda{}
	var gotReceipt receipt
	var gotErr error
	handler := func(ctx Context, _ string) (string, error) {
		r, err := Step(ctx, "charge", func(StepContext) (receipt, error) {
			return receipt{ID: "r1", Card: "4111111111111111"}, nil
		}, WithRetry(NoRetry()))
		if err != nil {
			return "", err
		}
		gotReceipt = r

		_, gotErr = Step(ctx, "label", func(StepContext) (string, error) {
			return "not a receipt", nil
		}, WithRetry(NoRetry()))
		return "", nil
	}

	h := Wrap(handler, withLambdaAPI(fake), WithSerdes(maskedReceiptSerdes("RCPT:")))
	if _, err := h(context.Background(), stepPayload(`""`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}

	if gotReceipt != (receipt{ID: "r1", Card: "****1111"}) {
		t.Errorf("Step(receipt) = %+v, want the masked receipt", gotReceipt)
	}

	if gotErr == nil {
		t.Fatal("Step(string) error = nil, want SerdesError for the second type")
	}
	var stepErr *StepError
	if !errors.As(gotErr, &stepErr) {
		t.Fatalf("Step(string) error = %v (%T), want *StepError", gotErr, gotErr)
	}
	if stepErr.ErrorType != "SerdesError" {
		t.Errorf("StepError.ErrorType = %q, want %q", stepErr.ErrorType, "SerdesError")
	}
	for _, want := range []string{"durable.receipt", "string"} {
		if !strings.Contains(stepErr.Message, want) {
			t.Errorf("StepError.Message = %q, want it to name %q", stepErr.Message, want)
		}
	}
}
