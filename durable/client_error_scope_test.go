package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// pagedPayload is an invocation payload whose embedded state page carries a
// NextMarker, so the SDK must call GetExecutionState to finish loading.
func pagedPayload() []byte {
	return []byte(`{
		"DurableExecutionArn": "arn:test",
		"CheckpointToken": "token-0",
		"InitialExecutionState": {
			"Operations": [{
				"Id": "exec-op",
				"Status": "STARTED",
				"ExecutionDetails": {"InputPayload": "\"evt\""}
			}],
			"NextMarker": "1"
		}
	}`)
}

// stepThenReturn is a handler that runs one step and returns whatever the
// step returned, passing a checkpoint failure through unchanged.
func stepThenReturn(ctx Context, _ string) (string, error) {
	return Step(ctx, "s", func(StepContext) (string, error) {
		return "ok", nil
	})
}

func parseResponse(t *testing.T, raw []byte) invocationResponse {
	t.Helper()
	var parsed invocationResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return parsed
}

func TestClientErrorExecutionScopeFromCheckpointFailsExecution(t *testing.T) {
	// The client states the failure is fatal for the execution. The SDK
	// does not retry the checkpoint, and the invocation responds FAILED.
	var calls atomic.Int32
	cause := errors.New("rejected")
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			calls.Add(1)
			return CheckpointOutput{}, &ClientError{Scope: ErrorScopeExecution, Err: cause}
		},
	}

	h := Wrap(stepThenReturn, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err != nil {
		t.Fatalf("Invoke error = %v, want FAILED response", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1 (execution scope is not retried)", got)
	}
	resp := parseResponse(t, raw)
	if resp.Status != invocationFailed {
		t.Fatalf("status = %q, want %q", resp.Status, invocationFailed)
	}
	if resp.Error == nil || resp.Error.ErrorType != "CheckpointError" {
		t.Errorf("error = %+v, want ErrorType CheckpointError", resp.Error)
	}
}

func TestClientErrorInvocationScopeFromCheckpointFailsInvocation(t *testing.T) {
	// The client states the failure is transient. The SDK retries the
	// checkpoint, and when every attempt fails the invocation ends with an
	// error rather than a FAILED response, so the execution resumes later.
	var calls atomic.Int32
	cause := errors.New("timeout")
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			calls.Add(1)
			return CheckpointOutput{}, &ClientError{Scope: ErrorScopeInvocation, Err: cause}
		},
	}

	h := Wrap(stepThenReturn, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	if got := calls.Load(); got != int32(checkpointMaxAttempts) {
		t.Errorf("checkpoint calls = %d, want %d", got, checkpointMaxAttempts)
	}
	var ce *CheckpointError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %v (%T), want wrapped *CheckpointError", err, err)
	}
	if ce.Scope() != ErrorScopeInvocation {
		t.Errorf("Scope() = %q, want %q", ce.Scope(), ErrorScopeInvocation)
	}
	if !ce.Retryable() || !IsCheckpointRetryable(err) {
		t.Error("invocation-scoped checkpoint error must report Retryable")
	}
	if !errors.Is(err, cause) {
		t.Error("error chain must reach the client's cause")
	}
}

func TestClientErrorZeroScopeFromCheckpointIsInvocation(t *testing.T) {
	// An unclassified ClientError is read as invocation-scoped: it is
	// retried and ends the invocation, not the execution.
	var calls atomic.Int32
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			calls.Add(1)
			return CheckpointOutput{}, &ClientError{Err: errors.New("unclassified")}
		},
	}

	h := Wrap(stepThenReturn, withLambdaAPI(fake))
	if raw, err := h(context.Background(), stepPayload(`"evt"`)); err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	if got := calls.Load(); got != int32(checkpointMaxAttempts) {
		t.Errorf("checkpoint calls = %d, want %d", got, checkpointMaxAttempts)
	}
}

func TestClientErrorExecutionScopeFromGetStateFailsExecution(t *testing.T) {
	// Loading state has no checkpoint classifier in front of it, so the
	// client's stated execution scope is read directly and the invocation
	// responds FAILED.
	cause := errors.New("execution not found")
	fake := &fakeLambdaFunc{
		getState: func(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
			return GetExecutionStateOutput{}, &ClientError{Scope: ErrorScopeExecution, Err: cause}
		},
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			t.Fatal("checkpoint must not be called when state loading fails")
			return CheckpointOutput{}, nil
		},
	}

	h := Wrap(func(Context, string) (string, error) {
		t.Fatal("handler must not run when state loading fails")
		return "", nil
	}, withLambdaAPI(fake))
	raw, err := h(context.Background(), pagedPayload())
	if err != nil {
		t.Fatalf("Invoke error = %v, want FAILED response", err)
	}
	resp := parseResponse(t, raw)
	if resp.Status != invocationFailed {
		t.Fatalf("status = %q, want %q", resp.Status, invocationFailed)
	}
	if resp.Error == nil || resp.Error.ErrorMessage == "" {
		t.Errorf("error = %+v, want a message", resp.Error)
	}
}

func TestClientErrorInvocationScopeFromGetStateFailsInvocation(t *testing.T) {
	// A transient failure loading state ends the invocation with an error;
	// the execution resumes in a later invocation.
	cause := errors.New("timeout")
	fake := &fakeLambdaFunc{
		getState: func(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error) {
			return GetExecutionStateOutput{}, &ClientError{Scope: ErrorScopeInvocation, Err: cause}
		},
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{}, nil
		},
	}

	h := Wrap(func(Context, string) (string, error) {
		t.Fatal("handler must not run when state loading fails")
		return "", nil
	}, withLambdaAPI(fake))
	raw, err := h(context.Background(), pagedPayload())
	if err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	if !errors.Is(err, cause) {
		t.Errorf("error = %v, want chain reaching the client's cause", err)
	}
}

func TestClientErrorExecutionScopeFromHandlerFailsExecution(t *testing.T) {
	// The scope is honored wherever the error surfaces: an execution-scoped
	// ClientError returned by handler code fails the execution.
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{CheckpointToken: "token-1"}, nil
		},
	}
	h := Wrap(func(Context, string) (string, error) {
		return "", &ClientError{Scope: ErrorScopeExecution, Err: errors.New("fatal")}
	}, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err != nil {
		t.Fatalf("Invoke error = %v, want FAILED response", err)
	}
	if resp := parseResponse(t, raw); resp.Status != invocationFailed {
		t.Fatalf("status = %q, want %q", resp.Status, invocationFailed)
	}
}

func TestClientErrorMessageAndUnwrap(t *testing.T) {
	cause := errors.New("boom")
	withCause := &ClientError{Scope: ErrorScopeExecution, Err: cause}
	if got, want := withCause.Error(), "durable: execution client: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(withCause, cause) {
		t.Error("errors.Is must reach the cause through Unwrap")
	}
	bare := &ClientError{}
	if got, want := bare.Error(), "durable: execution client error"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestClientErrorInvocationScopeFromHandlerFailsInvocation(t *testing.T) {
	// The scope is honored wherever the error surfaces: an
	// invocation-scoped ClientError returned by handler code ends the
	// invocation with an error, not a FAILED response, so the execution
	// resumes in a later invocation.
	cause := errors.New("transient")
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{CheckpointToken: "token-1"}, nil
		},
	}
	h := Wrap(func(Context, string) (string, error) {
		return "", &ClientError{Scope: ErrorScopeInvocation, Err: cause}
	}, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	if !errors.Is(err, cause) {
		t.Errorf("error = %v, want chain reaching the cause", err)
	}
}

func TestClientErrorInvocationScopeFromPluginFailsInvocation(t *testing.T) {
	// A WrapInvocation plugin sits between the handler and the outcome
	// translation. An invocation-scoped ClientError it returns is honored
	// the same way as one from the handler.
	cause := errors.New("transient")
	fake := &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(context.Context, CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{CheckpointToken: "token-1"}, nil
		},
	}
	plugin := Plugin{
		WrapInvocation: func(_ context.Context, _ InvocationHookInfo, fn func() (any, error)) (any, error) {
			if _, err := fn(); err != nil {
				return nil, err
			}
			return nil, &ClientError{Scope: ErrorScopeInvocation, Err: cause}
		},
	}
	h := Wrap(func(Context, string) (string, error) {
		return "ok", nil
	}, withLambdaAPI(fake), WithPlugins(plugin))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	if !errors.Is(err, cause) {
		t.Errorf("error = %v, want chain reaching the cause", err)
	}
}

func TestFailureScope(t *testing.T) {
	// failureScope reads a CheckpointError's known scope first, then a
	// ClientError's effective scope, and otherwise returns the caller's
	// default. Each case is run against both defaults so the table shows
	// which results depend on the default.
	tests := []struct {
		name string
		err  error
		// want is the resolved scope; "" means "whatever default was given".
		want ErrorScope
	}{
		{name: "nil", err: nil},
		{name: "plain error", err: errors.New("x")},
		{name: "rebuilt CheckpointError without scope", err: &CheckpointError{Err: errors.New("x")}},
		{name: "wrapped rebuilt CheckpointError", err: fmt.Errorf("w: %w", &CheckpointError{Err: errors.New("x")})},
		{name: "invocation CheckpointError", err: &CheckpointError{Err: errors.New("x"), scope: ErrorScopeInvocation}, want: ErrorScopeInvocation},
		{name: "execution CheckpointError", err: &CheckpointError{Err: errors.New("x"), scope: ErrorScopeExecution}, want: ErrorScopeExecution},
		{name: "invocation ClientError", err: &ClientError{Scope: ErrorScopeInvocation}, want: ErrorScopeInvocation},
		{name: "execution ClientError", err: &ClientError{Scope: ErrorScopeExecution}, want: ErrorScopeExecution},
		{name: "zero-scope ClientError", err: &ClientError{}, want: ErrorScopeInvocation},
		{name: "unknown-scope ClientError", err: &ClientError{Scope: ErrorScope("OTHER")}, want: ErrorScopeInvocation},
		{name: "wrapped execution ClientError", err: fmt.Errorf("w: %w", &ClientError{Scope: ErrorScopeExecution}), want: ErrorScopeExecution},
		{
			// A classified CheckpointError wraps the ClientError it was
			// classified from; the CheckpointError's scope is read first.
			name: "CheckpointError wrapping ClientError",
			err:  fmt.Errorf("w: %w", classifyCheckpointError(&ClientError{Scope: ErrorScopeExecution, Err: errors.New("x")})),
			want: ErrorScopeExecution,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, def := range []ErrorScope{ErrorScopeInvocation, ErrorScopeExecution} {
				want := tc.want
				if want == "" {
					want = def
				}
				if got := failureScope(tc.err, def); got != want {
					t.Errorf("failureScope(err, %q) = %q, want %q", def, got, want)
				}
			}
		})
	}
}

func TestCheckpointErrorScopeAccessors(t *testing.T) {
	inv := &CheckpointError{Err: errors.New("x"), scope: ErrorScopeInvocation}
	exe := &CheckpointError{Err: errors.New("y"), scope: ErrorScopeExecution}
	rebuilt := &CheckpointError{Err: errors.New("z")}

	if inv.Scope() != ErrorScopeInvocation || !inv.Retryable() {
		t.Error("invocation scope must be Retryable")
	}
	if exe.Scope() != ErrorScopeExecution || exe.Retryable() {
		t.Error("execution scope must not be Retryable")
	}
	if rebuilt.Scope() != "" || rebuilt.Retryable() {
		t.Error("a CheckpointError without a scope reports the zero scope and is not Retryable")
	}
	if failureScope(rebuilt, ErrorScopeExecution) != ErrorScopeExecution {
		t.Error("a CheckpointError without a scope states no scope, so the default applies")
	}
}
