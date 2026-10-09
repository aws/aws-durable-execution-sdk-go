package durable

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestClassifyCheckpointError(t *testing.T) {
	// Retryable is derived from the scope: true for the invocation scope,
	// except for a stale checkpoint token, which is invocation-scoped but
	// never retryable. Each case asserts scope, retryability, and the
	// stale-token mark so the relationship stays fixed.
	tests := []struct {
		name      string
		err       error
		scope     ErrorScope
		retryable bool
		stale     bool
	}{
		{
			// The service's exact message for a superseded token.
			name: "stale checkpoint token",
			err: &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: "Invalid checkpoint token",
				Fault:   smithy.FaultClient,
			},
			scope:     ErrorScopeInvocation,
			retryable: false,
			stale:     true,
		},
		{
			name: "stale checkpoint token with trailing detail",
			err: &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: "Invalid checkpoint token: superseded",
				Fault:   smithy.FaultClient,
			},
			scope:     ErrorScopeInvocation,
			retryable: false,
			stale:     true,
		},
		{
			// The prefix is compared case-sensitively. A message that
			// matches only without regard to case is an ordinary
			// invalid request.
			name: "stale-token message in another case",
			err: &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: "Invalid Checkpoint Token",
				Fault:   smithy.FaultClient,
			},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name: "stale-token message in lowercase",
			err: &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: "invalid checkpoint token: superseded",
				Fault:   smithy.FaultClient,
			},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			// KMS codes are server faults but fail the execution.
			name:      "KMSAccessDeniedException server fault",
			err:       &smithy.GenericAPIError{Code: "KMSAccessDeniedException", Fault: smithy.FaultServer},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name:      "KMSDisabledException server fault",
			err:       &smithy.GenericAPIError{Code: "KMSDisabledException", Fault: smithy.FaultServer},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name:      "KMSInvalidStateException server fault",
			err:       &smithy.GenericAPIError{Code: "KMSInvalidStateException", Fault: smithy.FaultServer},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name:      "wrapped KMSNotFoundException server fault",
			err:       fmt.Errorf("operation error: %w", &smithy.GenericAPIError{Code: "KMSNotFoundException", Fault: smithy.FaultServer}),
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			// A bare 429 with no modeled code is throttling.
			name: "bare HTTP 429",
			err: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusTooManyRequests}},
				Err:      errors.New("Too Many Requests"),
			},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name: "wrapped stale checkpoint token",
			err: fmt.Errorf("operation error Lambda: CheckpointDurableExecution: %w", &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: "Invalid checkpoint token",
				Fault:   smithy.FaultClient,
			}),
			scope:     ErrorScopeInvocation,
			retryable: false,
			stale:     true,
		},
		{
			// Same code, different message: an ordinary invalid request.
			name: "InvalidParameterValueException with another message",
			err: &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: "Invalid execution ARN",
				Fault:   smithy.FaultClient,
			},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			// Same message, different code: not the stale-token rejection.
			name: "stale-token message under another code",
			err: &smithy.GenericAPIError{
				Code:    "ValidationException",
				Message: "Invalid checkpoint token",
				Fault:   smithy.FaultClient,
			},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			// The client's statement wins over a stale-token-shaped cause.
			name: "client-stated execution scope over a stale-token cause",
			err: &ClientError{Scope: ErrorScopeExecution, Err: &smithy.GenericAPIError{
				Code:    "InvalidParameterValueException",
				Message: "Invalid checkpoint token",
				Fault:   smithy.FaultClient,
			}},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name:      "nil error",
			err:       nil,
			retryable: false, // returns nil, not checked
		},
		{
			name:      "client-stated invocation scope",
			err:       &ClientError{Scope: ErrorScopeInvocation, Err: errors.New("timeout")},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name:      "client-stated execution scope",
			err:       &ClientError{Scope: ErrorScopeExecution, Err: errors.New("rejected")},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name:      "client-stated zero scope defaults to invocation",
			err:       &ClientError{Err: errors.New("unclassified")},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name:      "client-stated unknown scope defaults to invocation",
			err:       &ClientError{Scope: ErrorScope("SOMETHING_ELSE"), Err: errors.New("unclassified")},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name:      "wrapped client-stated execution scope",
			err:       fmt.Errorf("transport: %w", &ClientError{Scope: ErrorScopeExecution, Err: errors.New("rejected")}),
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			// The client's statement wins over the shape of its cause.
			name: "client-stated execution scope over a server fault cause",
			err: &ClientError{Scope: ErrorScopeExecution, Err: &smithy.GenericAPIError{
				Code:  "ServiceException",
				Fault: smithy.FaultServer,
			}},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name: "server fault (5xx)",
			err: &smithy.GenericAPIError{
				Code:  "ServiceException",
				Fault: smithy.FaultServer,
			},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name: "client fault (4xx)",
			err: &smithy.GenericAPIError{
				Code:  "InvalidParameterValueException",
				Fault: smithy.FaultClient,
			},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name: "TooManyRequestsException (throttling, client fault but retryable)",
			err: &smithy.GenericAPIError{
				Code:  "TooManyRequestsException",
				Fault: smithy.FaultClient,
			},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name: "wrapped server fault",
			err: fmt.Errorf("operation error Lambda: CheckpointDurableExecution: %w", &smithy.GenericAPIError{
				Code:  "InternalServerError",
				Fault: smithy.FaultServer,
			}),
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name: "wrapped client fault",
			err: fmt.Errorf("operation error Lambda: CheckpointDurableExecution: %w", &smithy.GenericAPIError{
				Code:  "ResourceNotFoundException",
				Fault: smithy.FaultClient,
			}),
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name: "wrapped TooManyRequestsException",
			err: fmt.Errorf("outer: %w", &smithy.GenericAPIError{
				Code:  "TooManyRequestsException",
				Fault: smithy.FaultClient,
			}),
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name: "HTTP response error 500",
			err: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 500}},
				Err:      errors.New("internal"),
			},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name: "HTTP response error 400",
			err: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 400}},
				Err:      errors.New("bad request"),
			},
			scope:     ErrorScopeExecution,
			retryable: false,
		},
		{
			name:      "network error (no APIError)",
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name:      "context deadline exceeded",
			err:       context.DeadlineExceeded,
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name:      "plain error",
			err:       errors.New("something went wrong"),
			scope:     ErrorScopeInvocation,
			retryable: true,
		},
		{
			name: "unknown fault",
			err: &smithy.GenericAPIError{
				Code:  "UnknownException",
				Fault: smithy.FaultUnknown,
			},
			scope:     ErrorScopeExecution,
			retryable: false, // not FaultServer, not throttling → non-retryable
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil {
				if got := classifyCheckpointError(nil); got != nil {
					t.Fatalf("classifyCheckpointError(nil) = %v, want nil", got)
				}
				return
			}
			classified := classifyCheckpointError(tc.err)
			if classified == nil {
				t.Fatal("classifyCheckpointError returned nil for non-nil error")
			}
			if classified.Scope() != tc.scope {
				t.Errorf("Scope() = %q, want %q", classified.Scope(), tc.scope)
			}
			if classified.Retryable() != tc.retryable {
				t.Errorf("Retryable() = %v, want %v", classified.Retryable(), tc.retryable)
			}
			if classified.isStaleToken() != tc.stale {
				t.Errorf("isStaleToken() = %v, want %v", classified.isStaleToken(), tc.stale)
			}
			if !errors.Is(classified, tc.err) {
				t.Error("classified error should wrap original via errors.Is")
			}
		})
	}
}

func TestIsCheckpointRetryable(t *testing.T) {
	retryable := &CheckpointError{Err: errors.New("x"), scope: ErrorScopeInvocation}
	nonRetryable := &CheckpointError{Err: errors.New("y"), scope: ErrorScopeExecution}

	if !IsCheckpointRetryable(retryable) {
		t.Error("IsCheckpointRetryable should return true for retryable error")
	}
	if IsCheckpointRetryable(nonRetryable) {
		t.Error("IsCheckpointRetryable should return false for non-retryable error")
	}
	if IsCheckpointRetryable(errors.New("plain")) {
		t.Error("IsCheckpointRetryable should return false for non-CheckpointError")
	}
	if IsCheckpointRetryable(nil) {
		t.Error("IsCheckpointRetryable should return false for nil")
	}

	// Through wrapping.
	wrapped := fmt.Errorf("outer: %w", retryable)
	if !IsCheckpointRetryable(wrapped) {
		t.Error("IsCheckpointRetryable should work through wrapping")
	}
}

func TestCheckpointImmediateFailOnNonRetryable(t *testing.T) {
	nonRetryableErr := &smithy.GenericAPIError{
		Code:  "ValidationException",
		Fault: smithy.FaultClient,
	}

	var mu sync.Mutex
	callCount := 0
	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			mu.Lock()
			callCount++
			mu.Unlock()
			return CheckpointOutput{}, nonRetryableErr
		},
		getState: emptyGetState,
	}

	cp := newCheckpointer(fake, "arn:test", "token-0")
	err := cp.checkpoint(context.Background(), nil)
	if err == nil {
		t.Fatal("checkpoint() should fail on non-retryable error")
	}
	var ce *CheckpointError
	if !errors.As(err, &ce) {
		t.Fatalf("error should be *CheckpointError, got %T", err)
	}
	if ce.Retryable() {
		t.Error("error should be non-retryable")
	}
	mu.Lock()
	got := callCount
	mu.Unlock()
	if got != 1 {
		t.Errorf("expected 1 attempt (no retry), got %d", got)
	}
	if cp.currentToken() != "token-0" {
		t.Errorf("token = %q, want unchanged %q", cp.currentToken(), "token-0")
	}
}

func TestCheckpointRetryableFailureSentOnce(t *testing.T) {
	// The SDK makes one call per batch. The client's own retryer is the
	// only retry, so a fake client without one is called exactly once.
	serverErr := &smithy.GenericAPIError{
		Code:  "ServiceException",
		Fault: smithy.FaultServer,
	}

	var mu sync.Mutex
	callCount := 0
	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			mu.Lock()
			callCount++
			mu.Unlock()
			return CheckpointOutput{}, serverErr
		},
		getState: emptyGetState,
	}

	cp := newCheckpointer(fake, "arn:test", "token-0")
	err := cp.checkpoint(context.Background(), nil)
	if err == nil {
		t.Fatal("checkpoint() should fail")
	}
	var ce *CheckpointError
	if !errors.As(err, &ce) {
		t.Fatalf("error should be *CheckpointError, got %T", err)
	}
	if !ce.Retryable() {
		t.Error("error should still be classified as retryable")
	}
	mu.Lock()
	got := callCount
	mu.Unlock()
	if got != 1 {
		t.Errorf("checkpoint calls = %d, want 1", got)
	}
	if cp.currentToken() != "token-0" {
		t.Errorf("token = %q, want unchanged %q", cp.currentToken(), "token-0")
	}
}

func TestCheckpointTokenPreservedOnFailure(t *testing.T) {
	// Verifies that after a failed checkpoint (retryable),
	// the token remains at its pre-call value so the next attempt uses
	// the correct token.
	throttleErr := &smithy.GenericAPIError{
		Code:  "TooManyRequestsException",
		Fault: smithy.FaultClient,
	}

	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			// Verify the token being sent is always the initial one.
			if in.CheckpointToken != "token-initial" {
				t.Errorf("checkpoint sent wrong token: %q", in.CheckpointToken)
			}
			return CheckpointOutput{}, throttleErr
		},
		getState: emptyGetState,
	}

	cp := newCheckpointer(fake, "arn:test", "token-initial")
	err := cp.checkpoint(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if cp.currentToken() != "token-initial" {
		t.Errorf("token = %q, want %q", cp.currentToken(), "token-initial")
	}
}

func TestCheckpointContextCancellation(t *testing.T) {
	serverErr := &smithy.GenericAPIError{
		Code:  "ServiceException",
		Fault: smithy.FaultServer,
	}

	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, _ CheckpointInput) (CheckpointOutput, error) {
			return CheckpointOutput{}, serverErr
		},
		getState: emptyGetState,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	cp := newCheckpointer(fake, "arn:test", "token-0")
	err := cp.checkpoint(ctx, nil)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
	// The error should be context.Canceled (from the backoff select).
	if !errors.Is(err, context.Canceled) {
		// Or it could be the classified error if the first attempt already
		// returns the classified error before backoff. Both are acceptable.
		var ce *CheckpointError
		if !errors.As(err, &ce) {
			t.Fatalf("unexpected error type: %T: %v", err, err)
		}
	}
}

// --- helpers ---

// fakeLambdaFunc is a function-based ExecutionClient for fine-grained
// control in retry tests.
type fakeLambdaFunc struct {
	checkpoint func(context.Context, CheckpointInput) (CheckpointOutput, error)
	getState   func(context.Context, GetExecutionStateInput) (GetExecutionStateOutput, error)
}

func (f *fakeLambdaFunc) Checkpoint(ctx context.Context, in CheckpointInput) (CheckpointOutput, error) {
	return f.checkpoint(ctx, in)
}

func (f *fakeLambdaFunc) GetExecutionState(ctx context.Context, in GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return f.getState(ctx, in)
}

func emptyGetState(_ context.Context, _ GetExecutionStateInput) (GetExecutionStateOutput, error) {
	return GetExecutionStateOutput{}, nil
}

func TestCheckpointTokenRotationAfterFailedCall(t *testing.T) {
	// A failed call halts the checkpointer whatever its scope: it may
	// carry a START that no caller waits for, so no later call may be
	// sent. An invocation-scoped failure is recorded as the halt cause,
	// and every later checkpoint is refused without a call.
	// failed is set once the client has returned the failure. A call that
	// arrives after that is recorded in callAfterFailure.
	var failed, callAfterFailure atomic.Bool
	var firstToken atomic.Value

	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			if failed.Load() {
				callAfterFailure.Store(true)
				return CheckpointOutput{CheckpointToken: "token-after-failure"}, nil
			}
			firstToken.Store(in.CheckpointToken)
			failed.Store(true)
			return CheckpointOutput{}, &smithy.GenericAPIError{Code: "ServiceException", Fault: smithy.FaultServer}
		},
		getState: emptyGetState,
	}

	cp := newCheckpointer(fake, "arn:test", "token-0")
	err := cp.checkpoint(context.Background(), nil)
	var cpErr *CheckpointError
	if !errors.As(err, &cpErr) || cpErr.Scope() != ErrorScopeInvocation {
		t.Fatalf("first checkpoint() = %v, want an invocation-scoped *CheckpointError", err)
	}
	if got := firstToken.Load(); got != "token-0" {
		t.Errorf("failed call sent token %v, want token-0", got)
	}
	if halt := cp.haltCause(); halt != err {
		t.Errorf("haltCause() = %v, want the first call's error %v", halt, err)
	}
	if err := cp.checkpoint(context.Background(), nil); !errors.Is(err, errCheckpointTerminated) {
		t.Fatalf("checkpoint() after the failure = %v, want errCheckpointTerminated", err)
	}
	if callAfterFailure.Load() {
		t.Error("the client received a call after the failed call")
	}
}
