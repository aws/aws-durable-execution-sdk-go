package durable

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

// unixTimeSerdes is the delegation pattern from the JSONSerdes
// documentation: it handles time.Time itself and defers every other type
// to JSONSerdes.
type unixTimeSerdes struct{}

func (unixTimeSerdes) Marshal(ctx context.Context, meta SerdesContext, v any) ([]byte, error) {
	if t, ok := v.(time.Time); ok {
		return []byte(strconv.FormatInt(t.UnixNano(), 10)), nil
	}
	return JSONSerdes.Marshal(ctx, meta, v)
}

func (unixTimeSerdes) Unmarshal(ctx context.Context, meta SerdesContext, data []byte, v any) error {
	if t, ok := v.(*time.Time); ok {
		ns, err := strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return err
		}
		*t = time.Unix(0, ns)
		return nil
	}
	return JSONSerdes.Unmarshal(ctx, meta, data, v)
}

func TestJSONSerdesRoundTrip(t *testing.T) {
	in := receipt{ID: "r1", Card: "4111"}
	data, err := JSONSerdes.Marshal(context.Background(), SerdesContext{}, in)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if got := string(data); got != `{"id":"r1","card":"4111"}` {
		t.Errorf("Marshal() = %q, want encoding/json output", got)
	}
	var out receipt
	if err := JSONSerdes.Unmarshal(context.Background(), SerdesContext{}, data, &out); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	if out != in {
		t.Errorf("Unmarshal() = %+v, want %+v", out, in)
	}
}

func TestJSONSerdesIsTheDefault(t *testing.T) {
	// A context built with no serializer option carries the same serdes
	// the exported value wraps, so delegating to JSONSerdes reproduces
	// the SDK's default encoding exactly.
	ec := newTestContext(t, []*operation{execOp()})
	if ec.serdesDefaults().serdes != JSONSerdes {
		t.Errorf("default context serdes = %T, want JSONSerdes (%T)", ec.serdesDefaults().serdes, JSONSerdes)
	}

	// The callback path with a Deserializer marshals through the same
	// default.
	ds := deserializerSerdes{}
	got, err := ds.Marshal(context.Background(), SerdesContext{}, 42)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	want, _ := JSONSerdes.Marshal(context.Background(), SerdesContext{}, 42)
	if string(got) != string(want) {
		t.Errorf("deserializerSerdes.Marshal() = %q, want %q", got, want)
	}
}

func TestJSONSerdesDelegation(t *testing.T) {
	// A custom serdes handles one type and defers the rest: the special
	// type gets the custom encoding, every other type gets encoding/json.
	s := unixTimeSerdes{}
	ctx := context.Background()

	stamp := time.Unix(0, 1700000000123456789)
	data, err := s.Marshal(ctx, SerdesContext{}, stamp)
	if err != nil {
		t.Fatalf("Marshal(time.Time) error: %v", err)
	}
	if got := string(data); got != "1700000000123456789" {
		t.Errorf("Marshal(time.Time) = %q, want Unix nanoseconds", got)
	}
	var gotStamp time.Time
	if err := s.Unmarshal(ctx, SerdesContext{}, data, &gotStamp); err != nil {
		t.Fatalf("Unmarshal(*time.Time) error: %v", err)
	}
	if !gotStamp.Equal(stamp) {
		t.Errorf("Unmarshal(*time.Time) = %v, want %v", gotStamp, stamp)
	}

	data, err = s.Marshal(ctx, SerdesContext{}, receipt{ID: "r1", Card: "4111"})
	if err != nil {
		t.Fatalf("Marshal(receipt) error: %v", err)
	}
	if got := string(data); got != `{"id":"r1","card":"4111"}` {
		t.Errorf("Marshal(receipt) = %q, want the JSONSerdes encoding", got)
	}
	var gotReceipt receipt
	if err := s.Unmarshal(ctx, SerdesContext{}, data, &gotReceipt); err != nil {
		t.Fatalf("Unmarshal(*receipt) error: %v", err)
	}
	if gotReceipt != (receipt{ID: "r1", Card: "4111"}) {
		t.Errorf("Unmarshal(*receipt) = %+v, want the delegated decode", gotReceipt)
	}
}

func TestJSONSerdesDelegationHandlerWide(t *testing.T) {
	// The delegating serdes set with WithSerdes serves a step returning
	// the special type and a step returning an ordinary type in the same
	// handler, and both checkpoint payloads carry the expected encoding.
	fake := &fakeLambda{}
	stamp := time.Unix(0, 1700000000123456789)
	var gotStamp time.Time
	var gotReceipt receipt
	handler := func(ctx Context, _ string) (string, error) {
		s, err := Step(ctx, "when", func(StepContext) (time.Time, error) { return stamp, nil }, WithRetry(NoRetry()))
		if err != nil {
			return "", err
		}
		gotStamp = s
		r, err := Step(ctx, "charge", func(StepContext) (receipt, error) {
			return receipt{ID: "r1", Card: "4111"}, nil
		}, WithRetry(NoRetry()))
		if err != nil {
			return "", err
		}
		gotReceipt = r
		return "", nil
	}
	h := Wrap(handler, withLambdaAPI(fake), WithSerdes(unixTimeSerdes{}))
	if _, err := h(context.Background(), stepPayload(`""`)); err != nil {
		t.Fatalf("Invoke() error: %v", err)
	}
	if !gotStamp.Equal(stamp) {
		t.Errorf("Step(time.Time) = %v, want %v", gotStamp, stamp)
	}
	if gotReceipt != (receipt{ID: "r1", Card: "4111"}) {
		t.Errorf("Step(receipt) = %+v, want the receipt", gotReceipt)
	}

	payloads := map[string]string{}
	for _, u := range updateBatch(t, fake) {
		if u.Type == OperationTypeStep && u.Action == OperationActionSucceed && u.Id != nil && u.Payload != nil {
			payloads[*u.Id] = *u.Payload
		}
	}
	if got := payloads[hashID("1")]; got != "1700000000123456789" {
		t.Errorf("checkpointed time payload = %q, want Unix nanoseconds", got)
	}
	if got := payloads[hashID("2")]; got != `{"id":"r1","card":"4111"}` {
		t.Errorf("checkpointed receipt payload = %q, want JSON", got)
	}
}

func TestJSONSerdesConcurrentUse(t *testing.T) {
	// JSONSerdes holds no state, so concurrent Marshal and Unmarshal calls
	// from many goroutines are safe. The race detector checks this.
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := receipt{ID: strconv.Itoa(i), Card: "4111"}
			data, err := JSONSerdes.Marshal(context.Background(), SerdesContext{}, in)
			if err != nil {
				t.Errorf("Marshal() error: %v", err)
				return
			}
			var out receipt
			if err := JSONSerdes.Unmarshal(context.Background(), SerdesContext{}, data, &out); err != nil {
				t.Errorf("Unmarshal() error: %v", err)
				return
			}
			if out != in {
				t.Errorf("round trip = %+v, want %+v", out, in)
			}
		}()
	}
	wg.Wait()
}
