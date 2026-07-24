package durable

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestWithCallbackDeserializerWiring(t *testing.T) {
	// Verify that WithCallbackDeserializer is wired through handler options
	// to the callback deserialization path.

	// Custom deserializer that uppercases string values.
	deser := DeserializerFunc(func(data []byte, v any) error {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		p, ok := v.(*string)
		if !ok {
			t.Fatal("expected *string target")
		}
		*p = strings.ToUpper(s)
		return nil
	})

	// Verify option is stored.
	opts := handlerOptions{}
	WithCallbackDeserializer(deser).applyHandler(&opts)
	if opts.callbackDeserializer == nil {
		t.Fatal("callbackDeserializer not set")
	}

	// Verify callbackDeserializerForOptions uses it.
	ec := &execContext{serdes: jsonSerdes{}, callbackDeserializer: deser}
	serdes := callbackDeserializerForOptions(ec, callbackOptions{})
	var result string
	if err := serdes.Unmarshal(SerdesContext{}, []byte(`"hello"`), &result); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if result != "HELLO" {
		t.Errorf("result = %q, want HELLO", result)
	}
}

func TestCallbackDeserializerPrecedence(t *testing.T) {
	// Per-op WithCallbackSerdes takes precedence over handler-level.
	handlerDeser := DeserializerFunc(func(_ []byte, v any) error {
		p, ok := v.(*string)
		if !ok {
			t.Fatal("expected *string target")
		}
		*p = "from-handler"
		return nil
	})
	perOpSerdes := &testSerdes{suffix: "-perop"}

	ec := &execContext{serdes: jsonSerdes{}, callbackDeserializer: handlerDeser}

	// With per-op override.
	opts := callbackOptions{serdes: perOpSerdes}
	serdes := callbackDeserializerForOptions(ec, opts)
	var result string
	if err := serdes.Unmarshal(SerdesContext{}, []byte(`"test"`), &result); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if result != "test-perop" {
		t.Errorf("result = %q, want test-perop (per-op wins)", result)
	}
}

func TestWithLoggerOption(t *testing.T) {
	opts := handlerOptions{}
	l := NopLogger{}
	WithLogger(l).applyHandler(&opts)
	if opts.logger == nil {
		t.Fatal("logger not set")
	}
}

func TestWithSerdesOption(t *testing.T) {
	opts := handlerOptions{}
	s := &testSerdes{}
	WithSerdes(s).applyHandler(&opts)
	if opts.serdes == nil {
		t.Fatal("serdes not set")
	}
}

func TestHandlerOptionsConstructionTimeOnly(t *testing.T) {
	// Verify that all three main options (Logger, Serdes,
	// CallbackDeserializer) are captured at Wrap() time.
	l := NopLogger{}
	s := &testSerdes{}
	d := DeserializerFunc(func([]byte, any) error { return nil })

	h := Wrap(func(_ Context, _ string) (string, error) {
		return "", nil
	}, WithLogger(l), WithSerdes(s), WithCallbackDeserializer(d))

	dh, ok := h.(*durableHandler[string, string])
	if !ok {
		t.Fatal("Wrap did not return *durableHandler")
	}
	if dh.options.logger == nil {
		t.Error("logger not captured")
	}
	if dh.options.serdes == nil {
		t.Error("serdes not captured")
	}
	if dh.options.callbackDeserializer == nil {
		t.Error("callbackDeserializer not captured")
	}
}

// DeserializerFunc adapts a function to the Deserializer interface.
type DeserializerFunc func(data []byte, v any) error

func (f DeserializerFunc) Unmarshal(data []byte, v any) error { return f(data, v) }

// testSerdes appends a suffix on unmarshal for testing precedence.
type testSerdes struct {
	suffix string
}

func (s *testSerdes) Marshal(_ SerdesContext, v any) ([]byte, error) { return json.Marshal(v) }
func (s *testSerdes) Unmarshal(_ SerdesContext, data []byte, v any) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	p, ok := v.(*string)
	if !ok {
		return json.Unmarshal(data, v)
	}
	*p = raw + s.suffix
	return nil
}
