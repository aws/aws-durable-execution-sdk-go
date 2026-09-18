package durable

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	smithy "github.com/aws/smithy-go"
)

// staleTokenRejection is the service's rejection of a checkpoint token that
// a newer invocation has superseded, with the service's exact message.
func staleTokenRejection() error {
	return &smithy.GenericAPIError{
		Code:    "InvalidParameterValueException",
		Message: "Invalid checkpoint token",
		Fault:   smithy.FaultClient,
	}
}

// countingClient returns a client whose Checkpoint delegates to fn and
// counts its calls.
func countingClient(fn func(CheckpointInput) (CheckpointOutput, error)) (*fakeLambdaFunc, *atomic.Int32) {
	var calls atomic.Int32
	return &fakeLambdaFunc{
		getState: emptyGetState,
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			calls.Add(1)
			return fn(in)
		},
	}, &calls
}

// twoStepsThenReturn runs two steps in sequence and returns the second
// step's result, passing a checkpoint failure through unchanged.
func twoStepsThenReturn(ctx Context, _ string) (string, error) {
	if _, err := Step(ctx, "first", func(StepContext) (string, error) {
		return "one", nil
	}); err != nil {
		return "", err
	}
	return Step(ctx, "second", func(StepContext) (string, error) {
		return "two", nil
	})
}

// twoStepsSwallowErrors runs two steps in sequence and returns a success
// value even when a step fails, as handler code that mishandles errors
// would.
func twoStepsSwallowErrors(ctx Context, _ string) (string, error) {
	_, _ = Step(ctx, "first", func(StepContext) (string, error) {
		return "one", nil
	})
	_, _ = Step(ctx, "second", func(StepContext) (string, error) {
		return "two", nil
	})
	return "swallowed", nil
}

func TestStaleTokenIsNotRetriedAndHaltsCheckpointer(t *testing.T) {
	// A stale-token rejection is invocation-scoped but the token never
	// becomes valid again, so the checkpointer makes one call, returns the
	// classified error, and refuses every later checkpoint.
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, staleTokenRejection()
	})

	cp := newCheckpointer(fake, "arn:test", "token-0")
	err := cp.checkpoint(context.Background(), nil)
	if err == nil {
		t.Fatal("checkpoint() = nil, want stale-token error")
	}
	var ce *CheckpointError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %v (%T), want *CheckpointError", err, err)
	}
	if ce.Scope() != ErrorScopeInvocation {
		t.Errorf("Scope() = %q, want %q", ce.Scope(), ErrorScopeInvocation)
	}
	if ce.Retryable() {
		t.Error("stale-token error must not report Retryable")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1 (stale token is not retried)", got)
	}
	if cp.currentToken() != "token-0" {
		t.Errorf("token = %q, want unchanged token-0", cp.currentToken())
	}

	// Later calls are refused without reaching the client.
	if err := cp.checkpoint(context.Background(), nil); !errors.Is(err, errCheckpointTerminated) {
		t.Errorf("second checkpoint() = %v, want errCheckpointTerminated", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls after halt = %d, want 1", got)
	}
	if halt := cp.haltCause(); halt != err {
		t.Errorf("haltCause() = %v, want the stale-token error", halt)
	}
}

func TestStaleTokenEndsInvocationWithError(t *testing.T) {
	// The handler passes the stale-token error through. The invocation
	// ends with an error return, never a FAILED response, so the execution
	// continues in the invocation that holds the fresh token.
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, staleTokenRejection()
	})

	h := Wrap(twoStepsThenReturn, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	var ce *CheckpointError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %v (%T), want wrapped *CheckpointError", err, err)
	}
	if ce.Scope() != ErrorScopeInvocation {
		t.Errorf("Scope() = %q, want %q", ce.Scope(), ErrorScopeInvocation)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1", got)
	}
}

func TestStaleTokenEndsInvocationWithErrorWhenHandlerSwallowsIt(t *testing.T) {
	// Handler code that ignores the step error and returns a value cannot
	// turn a superseded invocation into a success: the recorded halt
	// cause overrides the handler's outcome.
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, staleTokenRejection()
	})

	h := Wrap(twoStepsSwallowErrors, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err == nil {
		t.Fatalf("Invoke returned response %s, want an error", raw)
	}
	var ce *CheckpointError
	if !errors.As(err, &ce) || ce.Scope() != ErrorScopeInvocation {
		t.Fatalf("error = %v, want invocation-scoped *CheckpointError", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1 (second step is refused locally)", got)
	}
}

func TestMissingTokenHaltsCheckpointer(t *testing.T) {
	// A response without a token means the service accepts no further
	// checkpoints from this invocation. The request receives
	// errCheckpointTerminated, the token is unchanged, and later calls are
	// refused without reaching the client.
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, nil
	})

	cp := newCheckpointer(fake, "arn:test", "token-0")
	if err := cp.checkpoint(context.Background(), nil); !errors.Is(err, errCheckpointTerminated) {
		t.Fatalf("checkpoint() = %v, want errCheckpointTerminated", err)
	}
	if cp.currentToken() != "token-0" {
		t.Errorf("token = %q, want unchanged token-0", cp.currentToken())
	}
	if err := cp.checkpoint(context.Background(), nil); !errors.Is(err, errCheckpointTerminated) {
		t.Errorf("second checkpoint() = %v, want errCheckpointTerminated", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1", got)
	}
	if halt := cp.haltCause(); !errors.Is(halt, errSuspendExecution) {
		t.Errorf("haltCause() = %v, want errSuspendExecution", halt)
	}
}

func TestMissingTokenEndsInvocationPending(t *testing.T) {
	// The first checkpoint response carries no token. The invocation
	// responds PENDING with no error, and no further checkpoint call is
	// made: the second step is refused locally.
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, nil
	})

	h := Wrap(twoStepsThenReturn, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err != nil {
		t.Fatalf("Invoke error = %v, want PENDING response", err)
	}
	resp := parseResponse(t, raw)
	if resp.Status != invocationPending {
		t.Fatalf("status = %q, want %q", resp.Status, invocationPending)
	}
	if resp.Error != nil {
		t.Errorf("error = %+v, want none", resp.Error)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1", got)
	}
}

func TestMissingTokenEndsInvocationPendingWhenHandlerSwallowsIt(t *testing.T) {
	// Handler code that ignores the step error and returns a value cannot
	// claim the execution finished: the invocation still responds PENDING.
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, nil
	})

	h := Wrap(twoStepsSwallowErrors, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`"evt"`))
	if err != nil {
		t.Fatalf("Invoke error = %v, want PENDING response", err)
	}
	resp := parseResponse(t, raw)
	if resp.Status != invocationPending {
		t.Fatalf("status = %q, want %q", resp.Status, invocationPending)
	}
	if resp.Result != nil {
		t.Errorf("result = %q, want none", *resp.Result)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1", got)
	}
}

func TestMissingTokenOnOversizedResultEndsInvocationPending(t *testing.T) {
	// The checkpoint that persists an oversized result gets a response
	// without a token. The invocation must not claim the execution
	// finished, so it responds PENDING and the next invocation replays.
	large := resultOfSerializedSize(lambdaResponseSizeLimit + 1)
	fake, calls := countingClient(func(CheckpointInput) (CheckpointOutput, error) {
		return CheckpointOutput{}, nil
	})

	h := Wrap(func(_ Context, _ string) (string, error) {
		return large, nil
	}, withLambdaAPI(fake))
	raw, err := h(context.Background(), stepPayload(`""`))
	if err != nil {
		t.Fatalf("Invoke error = %v, want PENDING response", err)
	}
	resp := parseResponse(t, raw)
	if resp.Status != invocationPending {
		t.Fatalf("status = %q, want %q", resp.Status, invocationPending)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checkpoint calls = %d, want 1", got)
	}
}
