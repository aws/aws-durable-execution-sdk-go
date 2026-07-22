package durable

import (
	"errors"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// CheckpointError wraps a checkpoint API failure with a retryability
// classification. Use [errors.As] to extract it from wrapped errors, and
// [CheckpointError.Retryable] to branch on retry decisions.
type CheckpointError struct {
	// Err is the original error from the checkpoint API call.
	Err error
	// retryable indicates whether the caller should retry the request.
	retryable bool
}

func (e *CheckpointError) Error() string {
	if e.Err != nil {
		return "durable: checkpoint: " + e.Err.Error()
	}
	return "durable: checkpoint error"
}

// Unwrap exposes the original cause to [errors.Is] and [errors.As].
func (e *CheckpointError) Unwrap() error { return e.Err }

// Retryable reports whether the checkpoint failure is transient and the
// caller should retry the request.
func (e *CheckpointError) Retryable() bool { return e.retryable }

// IsCheckpointRetryable reports whether err (or any error in its chain)
// is a retryable checkpoint failure. Returns false for nil.
func IsCheckpointRetryable(err error) bool {
	var ce *CheckpointError
	if errors.As(err, &ce) {
		return ce.retryable
	}
	return false
}

// classifyCheckpointError wraps err as a *CheckpointError with the
// appropriate retryability classification. Returns nil for nil input.
//
// Classification rules:
//  1. smithy.APIError in the chain:
//     - ErrorCode is "TooManyRequestsException" → retryable (throttling
//     is FaultClient in the generated code but is the canonical retry
//     case).
//     - ErrorFault is FaultServer → retryable (5xx server fault).
//     - Otherwise (FaultClient) → non-retryable (invalid request).
//  2. smithyhttp.ResponseError (HTTP response without a structured API
//     error): status >= 500 → retryable, otherwise non-retryable.
//  3. Anything else (network errors, context cancellation, timeouts) →
//     retryable. Transient conditions are the most common cause of
//     unstructured errors, and the retry loop is bounded.
func classifyCheckpointError(err error) *CheckpointError {
	if err == nil {
		return nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "TooManyRequestsException" {
			return &CheckpointError{Err: err, retryable: true}
		}
		if apiErr.ErrorFault() == smithy.FaultServer {
			return &CheckpointError{Err: err, retryable: true}
		}
		return &CheckpointError{Err: err, retryable: false}
	}

	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		return &CheckpointError{Err: err, retryable: respErr.HTTPStatusCode() >= 500}
	}

	// Unrecognized: network errors, context deadline, etc. — retryable.
	return &CheckpointError{Err: err, retryable: true}
}
