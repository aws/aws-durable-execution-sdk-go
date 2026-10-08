package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// markSerdes appends -MARK on Unmarshal and returns the bytes unchanged on
// Marshal, so a stored payload reveals which value produced it.
func markSerdes(suffix string) durable.Serdes {
	return durable.SerdesOf(
		func(_ context.Context, _ durable.SerdesContext, v string) ([]byte, error) {
			return []byte(v), nil
		},
		func(_ context.Context, _ durable.SerdesContext, data []byte) (string, error) {
			return string(data) + suffix, nil
		},
	)
}

// runWaitForCallbackToEnd runs handler until it blocks on its callback,
// completes the callback with payload, and runs it to the end.
func runWaitForCallbackToEnd[I any](t *testing.T, runner *durabletest.LocalRunner[I, string], event I, payload any) *durabletest.TestResult {
	t.Helper()
	r, err := runner.Run(event)
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
	r, err = runner.Run(event)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestWaitForCallbackStoresRawBytesAtChildContext pins where WaitForCallback
// applies the callback result serdes. The inner callback and the
// WaitForCallback child context both store the exact bytes the external
// system submitted, and the result serdes runs once at the WaitForCallback
// layer.
func TestWaitForCallbackStoresRawBytesAtChildContext(t *testing.T) {
	mark := markSerdes("-MARK")
	handler := func(ctx durable.Context, _ string) (string, error) {
		return durable.WaitForCallback[string](ctx, "wfcb",
			func(durable.StepContext, string) error { return nil },
			durable.WithCallbackSerdes(mark))
	}
	runner := durabletest.NewLocalRunner(handler)
	// SendCallbackSuccess JSON-encodes its argument, so the Go string
	// "payload" is stored as the wire bytes "payload" with quotes.
	r := runWaitForCallbackToEnd(t, runner, "", "payload")
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED; error = %+v", r.Status, r.Error)
	}

	got, err := durabletest.ResultAs[string](r)
	if err != nil {
		t.Fatal(err)
	}
	if got != `"payload"-MARK` {
		t.Fatalf("result = %q, want %q", got, `"payload"-MARK`)
	}

	cbs := r.OperationsByType(string(durable.OperationTypeCallback))
	if len(cbs) != 1 || cbs[0].CallbackDetails == nil {
		t.Fatalf("callback operations = %+v, want exactly one with details", cbs)
	}
	if cbs[0].CallbackDetails.Result != `"payload"` {
		t.Fatalf("callback stored result = %q, want %q (the submitted bytes)",
			cbs[0].CallbackDetails.Result, `"payload"`)
	}

	ctxs := r.OperationsByType(string(durable.OperationTypeContext))
	if len(ctxs) != 1 || ctxs[0].ContextDetails == nil {
		t.Fatalf("context operations = %+v, want exactly one with details", ctxs)
	}
	if ctxs[0].ContextDetails.Result != `"payload"` {
		t.Fatalf("child-context stored result = %q, want %q (the submitted bytes)",
			ctxs[0].ContextDetails.Result, `"payload"`)
	}
}

// TestWaitForCallbackReplayReturnsSameValue pins that a replayed SUCCEEDED
// WaitForCallback applies the result serdes to the stored submitted bytes
// and returns the value the first run returned.
func TestWaitForCallbackReplayReturnsSameValue(t *testing.T) {
	mark := markSerdes("-MARK")
	var values []string
	handler := func(ctx durable.Context, _ string) (string, error) {
		v, err := durable.WaitForCallback[string](ctx, "wfcb",
			func(durable.StepContext, string) error { return nil },
			durable.WithCallbackSerdes(mark))
		if err != nil {
			return "", err
		}
		values = append(values, v)
		// A wait after the callback forces one more invocation, which
		// replays the SUCCEEDED WaitForCallback.
		if err := durable.Wait(ctx, "after", time.Second); err != nil {
			return "", err
		}
		return v, nil
	}
	runner := durabletest.NewLocalRunner(handler)
	r := runWaitForCallbackToEnd(t, runner, "", "payload")
	if r.Status != durabletest.Pending {
		t.Fatalf("status after callback = %s, want PENDING on the wait", r.Status)
	}
	r, err := runner.RunUntilComplete("")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != durabletest.Succeeded {
		t.Fatalf("status = %s, want SUCCEEDED; error = %+v", r.Status, r.Error)
	}
	if len(values) < 2 {
		t.Fatalf("WaitForCallback returned %d times, want at least 2 (first run and replay)", len(values))
	}
	for i, v := range values {
		if v != `"payload"-MARK` {
			t.Errorf("run %d returned %q, want %q", i, v, `"payload"-MARK`)
		}
	}
}

// TestWaitForCallbackSerdesFailureNamesOperation pins that a result serdes
// whose Unmarshal fails ends the execution FAILED with a *SerdesError that
// names the WaitForCallback operation.
func TestWaitForCallbackSerdesFailureNamesOperation(t *testing.T) {
	failing := durable.SerdesOf(
		func(_ context.Context, _ durable.SerdesContext, v string) ([]byte, error) {
			return []byte(v), nil
		},
		func(_ context.Context, _ durable.SerdesContext, _ []byte) (string, error) {
			return "", errors.New("cannot decode")
		},
	)
	var got error
	handler := func(ctx durable.Context, _ string) (string, error) {
		v, err := durable.WaitForCallback[string](ctx, "wfcb",
			func(durable.StepContext, string) error { return nil },
			durable.WithCallbackSerdes(failing))
		got = err
		return v, err
	}
	runner := durabletest.NewLocalRunner(handler)
	r := runWaitForCallbackToEnd(t, runner, "", "payload")
	if r.Status != durabletest.Failed {
		t.Fatalf("status = %s, want FAILED", r.Status)
	}
	var serr *durable.SerdesError
	if !errors.As(got, &serr) {
		t.Fatalf("WaitForCallback error = %v (%T), want *SerdesError", got, got)
	}
	if serr.Operation != "wfcb" {
		t.Errorf("SerdesError.Operation = %q, want %q", serr.Operation, "wfcb")
	}
	if r.Error == nil || r.Error.Type != "SerdesError" {
		t.Errorf("execution error = %+v, want Type SerdesError", r.Error)
	}
	// The serdes runs at the WaitForCallback layer, so the inner callback
	// and the child context both recorded success with the submitted bytes.
	ctxs := r.OperationsByType(string(durable.OperationTypeContext))
	if len(ctxs) != 1 || ctxs[0].ContextDetails == nil || ctxs[0].ContextDetails.Result != `"payload"` {
		t.Errorf("context operations = %+v, want one storing the submitted bytes", ctxs)
	}
}

// TestWaitForCallbackSerdesPrecedence pins that the handler-level
// WithCallbackDeserializer and the per-operation WithCallbackSerdes both
// select the serdes WaitForCallback applies, the per-operation option
// first, and that neither changes the stored bytes.
func TestWaitForCallbackSerdesPrecedence(t *testing.T) {
	var handlerDeser durable.Deserializer = suffixDeserializer("-HANDLER")
	cases := []struct {
		name  string
		opts  []durable.WaitForCallbackOption
		hopts []durable.HandlerOption
		want  string
	}{
		{name: "default", want: `"payload"`},
		{name: "handler", hopts: []durable.HandlerOption{durable.WithCallbackDeserializer(handlerDeser)}, want: `"payload"-HANDLER`},
		{name: "operation", opts: []durable.WaitForCallbackOption{durable.WithCallbackSerdes(markSerdes("-OP"))}, want: `"payload"-OP`},
		{
			name:  "operation over handler",
			opts:  []durable.WaitForCallbackOption{durable.WithCallbackSerdes(markSerdes("-OP"))},
			hopts: []durable.HandlerOption{durable.WithCallbackDeserializer(handlerDeser)},
			want:  `"payload"-OP`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := func(ctx durable.Context, _ string) (string, error) {
				return durable.WaitForCallback[string](ctx, "wfcb",
					func(durable.StepContext, string) error { return nil }, tc.opts...)
			}
			runner := durabletest.NewLocalRunner(handler, tc.hopts...)
			r := runWaitForCallbackToEnd(t, runner, "", "payload")
			if r.Status != durabletest.Succeeded {
				t.Fatalf("status = %s, want SUCCEEDED; error = %+v", r.Status, r.Error)
			}
			got, err := durabletest.ResultAs[string](r)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("result = %q, want %q", got, tc.want)
			}
			for _, typ := range []durable.OperationType{durable.OperationTypeCallback, durable.OperationTypeContext} {
				for _, op := range r.OperationsByType(string(typ)) {
					stored := ""
					if op.CallbackDetails != nil {
						stored = op.CallbackDetails.Result
					}
					if op.ContextDetails != nil {
						stored = op.ContextDetails.Result
					}
					if stored != `"payload"` {
						t.Errorf("%s operation stored %q, want the submitted bytes %q", typ, stored, `"payload"`)
					}
				}
			}
		})
	}
}

// suffixDeserializer is a handler-level callback deserializer that appends
// its value to the submitted bytes.
type suffixDeserializer string

func (s suffixDeserializer) Unmarshal(data []byte, v any) error {
	p, ok := v.(*string)
	if !ok {
		return errors.New("suffixDeserializer: want *string")
	}
	*p = string(data) + string(s)
	return nil
}
