package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// TestCallbackDefaultDeserializerIsPassThrough pins the required default: a
// callback created with no serdes option returns the exact bytes the
// external system submitted, matching the JavaScript and Python SDKs.
func TestCallbackDefaultDeserializerIsPassThrough(t *testing.T) {
	handler := func(ctx durable.Context, _ string) (string, error) {
		cb, err := durable.CreateCallback[string](ctx, "cb")
		if err != nil {
			return "", err
		}
		return cb.Result(ctx)
	}
	// run completes the callback with payload, which SendCallbackSuccess
	// JSON-encodes, and returns the handler's string result.
	run := func(t *testing.T, payload any) string {
		t.Helper()
		runner := durabletest.NewLocalRunner(handler)
		r, err := runner.Run("")
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Pending {
			t.Fatalf("first invocation = %s, want PENDING", r.Status)
		}
		open := runner.OpenCallbacks()
		if len(open) != 1 {
			t.Fatalf("open callbacks = %d, want 1", len(open))
		}
		if err := runner.SendCallbackSuccess(open[0].CallbackID, payload); err != nil {
			t.Fatal(err)
		}
		r, err = runner.Run("")
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Succeeded {
			t.Fatalf("status = %s, want SUCCEEDED; error = %+v", r.Status, r.Error)
		}
		got, err := durabletest.ResultAs[string](r)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	// Case A: the Go string "approved" is stored as the wire bytes
	// "approved" with quotes. The default returns them unchanged.
	t.Run("string payload keeps the raw bytes", func(t *testing.T) {
		if got := run(t, "approved"); got != `"approved"` {
			t.Fatalf("result = %q, want %q (the raw submitted bytes)", got, `"approved"`)
		}
	})

	// Case B: 42 is stored as the two bytes 42. The default returns the
	// string 42.
	t.Run("number payload into a string result", func(t *testing.T) {
		if got := run(t, 42); got != "42" {
			t.Fatalf("result = %q, want %q (the raw submitted bytes)", got, "42")
		}
	})
}

// TestRawSerdesRejectsUnsupportedTypes asserts that an operation using
// RawSerdes fails with a *SerdesError naming the Go type when the value to
// marshal, or the target to unmarshal into, is not a string, []byte, or
// json.RawMessage.
func TestRawSerdesRejectsUnsupportedTypes(t *testing.T) {
	check := func(t *testing.T, err error, direction, typeName string) {
		t.Helper()
		var serdesErr *durable.SerdesError
		if !errors.As(err, &serdesErr) {
			t.Fatalf("error = %T (%v), want *SerdesError", err, err)
		}
		if serdesErr.Direction != direction {
			t.Errorf("Direction = %q, want %q", serdesErr.Direction, direction)
		}
		if !strings.Contains(serdesErr.Error(), typeName) {
			t.Errorf("error %q does not name the type %s", serdesErr.Error(), typeName)
		}
	}

	t.Run("marshal", func(t *testing.T) {
		var childErr error
		handler := func(ctx durable.Context, _ string) (string, error) {
			_, childErr = durable.RunInChildContext(ctx, "count", func(durable.Context) (int, error) {
				return 7, nil
			}, durable.WithChildSerdes(durable.RawSerdes))
			return "", childErr
		}
		r, err := durabletest.NewLocalRunner(handler).RunUntilComplete("")
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Failed {
			t.Fatalf("status = %s, want FAILED", r.Status)
		}
		check(t, childErr, "marshal", "int")
	})

	t.Run("unmarshal", func(t *testing.T) {
		var resultErr error
		handler := func(ctx durable.Context, _ string) (string, error) {
			cb, err := durable.CreateCallback[int](ctx, "cb")
			if err != nil {
				return "", err
			}
			_, resultErr = cb.Result(ctx)
			return "", resultErr
		}
		runner := durabletest.NewLocalRunner(handler)
		if _, err := runner.Run(""); err != nil {
			t.Fatal(err)
		}
		open := runner.OpenCallbacks()
		if len(open) != 1 {
			t.Fatalf("open callbacks = %d, want 1", len(open))
		}
		if err := runner.SendCallbackSuccess(open[0].CallbackID, 42); err != nil {
			t.Fatal(err)
		}
		r, err := runner.Run("")
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != durabletest.Failed {
			t.Fatalf("status = %s, want FAILED", r.Status)
		}
		check(t, resultErr, "unmarshal", "*int")
	})
}

// TestRawSerdesRoundTrip asserts that RawSerdes passes the supported types
// through unchanged in both directions.
func TestRawSerdesRoundTrip(t *testing.T) {
	ctx := context.Background()
	meta := durable.SerdesContext{}
	for _, v := range []any{`"quoted"`, []byte("bytes"), json.RawMessage(`{"a":1}`)} {
		b, err := durable.RawSerdes.Marshal(ctx, meta, v)
		if err != nil {
			t.Fatalf("Marshal(%T): %v", v, err)
		}
		if got, want := string(b), fmt.Sprint(rawString(v)); got != want {
			t.Errorf("Marshal(%T) = %q, want %q", v, got, want)
		}
	}
	data := []byte(`"x"`)
	var s string
	var bs []byte
	var raw json.RawMessage
	for _, target := range []any{&s, &bs, &raw} {
		if err := durable.RawSerdes.Unmarshal(ctx, meta, data, target); err != nil {
			t.Fatalf("Unmarshal(%T): %v", target, err)
		}
	}
	if s != `"x"` || string(bs) != `"x"` || string(raw) != `"x"` {
		t.Errorf("Unmarshal results = %q %q %q, want %q each", s, bs, raw, `"x"`)
	}
	if err := durable.RawSerdes.Unmarshal(ctx, meta, data, (*string)(nil)); err == nil {
		t.Error("Unmarshal into a nil *string returned nil error")
	}
}

func rawString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case json.RawMessage:
		return string(x)
	}
	return ""
}
