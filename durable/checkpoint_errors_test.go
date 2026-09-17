package durable

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestClassifyCheckpointError(t *testing.T) {
	// Retryable is derived from the scope: true exactly for the invocation
	// scope. Each case asserts both so the relationship stays fixed.
	tests := []struct {
		name      string
		err       error
		scope     ErrorScope
		retryable bool
	}{
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

func TestCheckpointRetryOnRetryableError(t *testing.T) {
	retryableErr := &smithy.GenericAPIError{
		Code:  "ServiceException",
		Fault: smithy.FaultServer,
	}

	var mu sync.Mutex
	callCount := 0
	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			mu.Lock()
			callCount++
			n := callCount
			mu.Unlock()
			if n < 3 {
				return CheckpointOutput{}, retryableErr
			}
			tok := "token-success"
			return CheckpointOutput{CheckpointToken: tok}, nil
		},
		getState: emptyGetState,
	}

	cp := newCheckpointer(fake, "arn:test", "token-0")
	err := cp.checkpoint(context.Background(), nil)
	if err != nil {
		t.Fatalf("checkpoint() should succeed after retries, got: %v", err)
	}
	mu.Lock()
	got := callCount
	mu.Unlock()
	if got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
	if cp.currentToken() != "token-success" {
		t.Errorf("token = %q, want %q", cp.currentToken(), "token-success")
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

func TestCheckpointExhaustsRetriesOnRetryable(t *testing.T) {
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
		t.Fatal("checkpoint() should fail after exhausting retries")
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
	if got != checkpointMaxAttempts {
		t.Errorf("expected %d attempts, got %d", checkpointMaxAttempts, got)
	}
	if cp.currentToken() != "token-0" {
		t.Errorf("token = %q, want unchanged %q", cp.currentToken(), "token-0")
	}
}

func TestCheckpointTokenPreservedOnFailure(t *testing.T) {
	// Verifies that after a failed checkpoint (retryable, exhausted),
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
				t.Errorf("retry sent wrong token: %q", in.CheckpointToken)
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

// Verify the existing token rotation tests still exercise the old
// behavior (non-classified errors from fakeLambda still propagate through
// the retry logic and are classified).
func TestCheckpointRetryWithTokenRotation(t *testing.T) {
	// After a successful checkpoint preceded by retryable failures,
	// subsequent calls use the new token.
	var mu sync.Mutex
	callCount := 0
	tokens := []string{}

	fake := &fakeLambdaFunc{
		checkpoint: func(_ context.Context, in CheckpointInput) (CheckpointOutput, error) {
			mu.Lock()
			callCount++
			n := callCount
			tokens = append(tokens, in.CheckpointToken)
			mu.Unlock()
			if n == 1 {
				return CheckpointOutput{}, &smithy.GenericAPIError{Code: "ServiceException", Fault: smithy.FaultServer}
			}
			tok := "token-" + strconv.Itoa(n)
			return CheckpointOutput{
				CheckpointToken: tok,
			}, nil
		},
		getState: emptyGetState,
	}

	cp := newCheckpointer(fake, "arn:test", "token-0")
	if err := cp.checkpoint(context.Background(), nil); err != nil {
		t.Fatalf("checkpoint() = %v", err)
	}
	if cp.currentToken() != "token-2" {
		t.Errorf("token = %q, want %q", cp.currentToken(), "token-2")
	}

	// Second call should use the rotated token.
	if err := cp.checkpoint(context.Background(), []OperationUpdate{}); err != nil {
		t.Fatalf("checkpoint() 2 = %v", err)
	}
	mu.Lock()
	if len(tokens) < 3 {
		t.Fatalf("expected at least 3 calls, got %d", len(tokens))
	}
	if tokens[2] != "token-2" {
		t.Errorf("third call used token %q, want %q", tokens[2], "token-2")
	}
	mu.Unlock()
}
