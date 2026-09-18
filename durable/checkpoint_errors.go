package durable

import (
	"errors"
	"strings"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ErrorScope states how far an [ExecutionClient] failure reaches: whether
// it ends only the current invocation or the whole execution.
//
// The SDK decides, for every failed client call, between two outcomes. An
// invocation-scoped failure ends the current invocation with an error, and
// the execution resumes in a later invocation from its last checkpoint. An
// execution-scoped failure ends the execution: the invocation responds
// FAILED and no later invocation follows.
type ErrorScope string

const (
	// ErrorScopeInvocation means the current invocation cannot continue but
	// the execution can resume in a new one. It fits transient conditions:
	// timeouts, throttling, connection failures, and server-side errors.
	// The SDK retries a checkpoint call that fails with this scope before
	// giving up on the invocation, except for a stale checkpoint token,
	// which a newer invocation has superseded and no retry can revive.
	ErrorScopeInvocation ErrorScope = "INVOCATION"

	// ErrorScopeExecution means the execution cannot proceed and must fail.
	// It fits conditions a retry cannot resolve: a rejected request,
	// missing permissions, an unknown or finished execution, or
	// misconfiguration. The SDK does not retry a call that fails with this
	// scope.
	ErrorScopeExecution ErrorScope = "EXECUTION"
)

// ClientError is the error an [ExecutionClient] returns to state the scope
// of a failure directly. The default client infers the scope from the AWS
// SDK's error shape. A custom client that does not produce AWS-shaped
// errors has no other way to express the distinction: without it, every
// failure is treated as transient, so a permanent failure is retried until
// the execution times out.
//
// The SDK honors the scope wherever the error surfaces. A ClientError
// returned from [ExecutionClient.Checkpoint] is wrapped in a
// [CheckpointError] carrying the same scope. A ClientError returned from
// [ExecutionClient.GetExecutionState] with [ErrorScopeExecution] fails the
// execution instead of the invocation. A ClientError that handler or
// plugin code returns is acted on like a CheckpointError of the same
// scope: [ErrorScopeInvocation] ends the invocation with an error so the
// execution resumes later, and [ErrorScopeExecution] fails the execution.
//
// A Scope that is neither [ErrorScopeInvocation] nor [ErrorScopeExecution],
// including the zero value, is treated as [ErrorScopeInvocation]. Assuming a
// failure is transient is the safe default: the execution gets another
// attempt rather than being failed on the strength of an error the SDK does
// not understand.
//
//	func (c *httpClient) Checkpoint(ctx context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
//		resp, err := c.post(ctx, in)
//		if err != nil {
//			return durable.CheckpointOutput{}, &durable.ClientError{Scope: durable.ErrorScopeInvocation, Err: err}
//		}
//		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
//			return durable.CheckpointOutput{}, &durable.ClientError{Scope: durable.ErrorScopeExecution, Err: errors.New(resp.Status)}
//		}
//		...
//	}
type ClientError struct {
	// Scope states how far the failure reaches.
	Scope ErrorScope

	// Err is the underlying failure, kept for diagnostics.
	Err error
}

func (e *ClientError) Error() string {
	if e.Err != nil {
		return "durable: execution client: " + e.Err.Error()
	}
	return "durable: execution client error"
}

// Unwrap exposes the underlying failure to [errors.Is] and [errors.As].
func (e *ClientError) Unwrap() error { return e.Err }

// effectiveScope returns the scope the SDK acts on for a ClientError: the
// stated scope when it is one of the two known values, and
// [ErrorScopeInvocation] otherwise.
func (e *ClientError) effectiveScope() ErrorScope {
	if e.Scope == ErrorScopeExecution {
		return ErrorScopeExecution
	}
	return ErrorScopeInvocation
}

// CheckpointError wraps a checkpoint API failure with its [ErrorScope]. Use
// [errors.As] to extract it from wrapped errors, [CheckpointError.Scope] to
// see how far the failure reaches, and [CheckpointError.Retryable] to
// branch on retry decisions.
//
// # Retryability and scope
//
// Retryable is derived from the scope: it is true when Scope is
// [ErrorScopeInvocation], with one exception. An invocation-scoped failure
// is transient, so the SDK retries the checkpoint call before giving up on
// the invocation. An execution-scoped failure is permanent, so the SDK does
// not retry it.
//
// The exception is a stale checkpoint token. The service rejects a
// checkpoint whose token a newer invocation has superseded. The failure is
// invocation-scoped: the execution continues in the newer invocation, so
// the current one ends with an error and nothing is lost. But the token
// never becomes valid again, so Retryable is false and the SDK does not
// retry the call.
//
// # How the SDK acts on the scope
//
// When a CheckpointError escapes the handler, the SDK reads its scope. An
// invocation-scoped error ends the invocation with an error, so the
// execution resumes in a later invocation from its last checkpoint. An
// execution-scoped error ends the execution with a FAILED response. Handler
// code that wants an ordinary failure should return its own error rather
// than pass a CheckpointError through.
//
// A stale-token rejection ends the invocation with an error even when the
// handler does not pass it through. The SDK stops checkpointing the moment
// the rejection arrives, and the invocation's outcome cannot be reported
// with a token the service no longer accepts.
//
// The same rule applies to the checkpoint the SDK makes on the handler's
// behalf when a result is too large to return inline: an invocation-scoped
// failure ends the invocation with an error, and an execution-scoped
// failure ends the execution with a FAILED response.
//
// A CheckpointError rebuilt from a checkpoint record by [ErrorFromObject]
// carries no scope: Scope is the zero value and Retryable is false. The
// record does not store the classification.
type CheckpointError struct {
	// Err is the original error from the checkpoint API call.
	Err error
	// scope states how far the failure reaches.
	scope ErrorScope
	// staleToken is set when the service rejected the checkpoint token as
	// superseded. The failure is invocation-scoped but not retryable.
	staleToken bool
}

func (e *CheckpointError) Error() string {
	if e.Err != nil {
		return "durable: checkpoint: " + e.Err.Error()
	}
	return "durable: checkpoint error"
}

// Unwrap exposes the original cause to [errors.Is] and [errors.As].
func (e *CheckpointError) Unwrap() error { return e.Err }

// Scope reports how far the checkpoint failure reaches. See [ErrorScope].
func (e *CheckpointError) Scope() ErrorScope { return e.scope }

// Retryable reports whether the checkpoint failure is transient and the
// caller should retry the request. It is true when [Scope] is
// [ErrorScopeInvocation], except for a stale checkpoint token, which no
// retry can make valid again.
func (e *CheckpointError) Retryable() bool {
	return e.scope == ErrorScopeInvocation && !e.staleToken
}

// isStaleToken reports whether the failure is the service's rejection of a
// superseded checkpoint token.
func (e *CheckpointError) isStaleToken() bool { return e.staleToken }

// IsCheckpointRetryable reports whether err (or any error in its chain)
// is a retryable checkpoint failure. Returns false for nil.
func IsCheckpointRetryable(err error) bool {
	var ce *CheckpointError
	if errors.As(err, &ce) {
		return ce.Retryable()
	}
	return false
}

// classifyCheckpointError wraps err as a *CheckpointError carrying the
// scope of the failure. Returns nil for nil input.
//
// Classification rules, applied in order:
//  1. *ClientError in the chain: the client stated the scope, so it is
//     used as is. A client that states its own scope does not have to
//     imitate the AWS SDK's error shape. See [ClientError] for how an
//     unknown scope value is read.
//  2. smithy.APIError in the chain:
//     - ErrorCode is "InvalidParameterValueException" and the message
//     starts with "Invalid checkpoint token" (compared without regard to
//     case) → invocation scope, marked as a stale token. A newer
//     invocation has superseded this one, so the execution continues
//     there. The token never becomes valid again, so the error is not
//     retryable.
//     - ErrorCode is "TooManyRequestsException" → invocation scope
//     (throttling is FaultClient in the generated code but is the
//     canonical retry case).
//     - ErrorFault is FaultServer → invocation scope (5xx server fault).
//     - Otherwise (FaultClient) → execution scope (invalid request).
//  3. smithyhttp.ResponseError (HTTP response without a structured API
//     error): status >= 500 → invocation scope, otherwise execution scope.
//  4. Anything else (network errors, context cancellation, timeouts) →
//     invocation scope. Transient conditions are the most common cause of
//     unstructured errors, the retry loop is bounded, and assuming a
//     failure is transient is the safe default: the execution gets another
//     attempt rather than being failed on the strength of an error the SDK
//     does not understand.
func classifyCheckpointError(err error) *CheckpointError {
	if err == nil {
		return nil
	}
	if isStaleTokenRejection(err) {
		return &CheckpointError{Err: err, scope: ErrorScopeInvocation, staleToken: true}
	}
	return &CheckpointError{Err: err, scope: clientErrorScope(err)}
}

// staleTokenErrorCode and staleTokenMessagePrefix identify the service's
// rejection of a checkpoint token that a newer invocation has superseded.
const (
	staleTokenErrorCode     = "InvalidParameterValueException"
	staleTokenMessagePrefix = "invalid checkpoint token"
)

// isStaleTokenRejection reports whether err is the service's rejection of
// a superseded checkpoint token: a smithy.APIError with code
// [staleTokenErrorCode] whose message starts with
// [staleTokenMessagePrefix], compared without regard to case. A
// *ClientError in the chain takes precedence: the client stated the scope
// itself, so the shape of its cause is not inspected.
func isStaleTokenRejection(err error) bool {
	var clientErr *ClientError
	if errors.As(err, &clientErr) {
		return false
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.ErrorCode() != staleTokenErrorCode {
		return false
	}
	msg := apiErr.ErrorMessage()
	return len(msg) >= len(staleTokenMessagePrefix) &&
		strings.EqualFold(msg[:len(staleTokenMessagePrefix)], staleTokenMessagePrefix)
}

// clientErrorScope derives the [ErrorScope] of a failed [ExecutionClient]
// call from the error it returned, by the rules listed on
// [classifyCheckpointError].
func clientErrorScope(err error) ErrorScope {
	var clientErr *ClientError
	if errors.As(err, &clientErr) {
		return clientErr.effectiveScope()
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "TooManyRequestsException" {
			return ErrorScopeInvocation
		}
		if apiErr.ErrorFault() == smithy.FaultServer {
			return ErrorScopeInvocation
		}
		return ErrorScopeExecution
	}

	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		if respErr.HTTPStatusCode() >= 500 {
			return ErrorScopeInvocation
		}
		return ErrorScopeExecution
	}

	// Unrecognized: network errors, context deadline, etc.
	return ErrorScopeInvocation
}

// failureScope returns the [ErrorScope] the SDK acts on when err ends an
// invocation. Every site that turns a failure into a wire outcome reads
// the scope through this function, so a scope is honored the same way
// wherever the error surfaces.
//
// The scope comes from the first of these found in err's chain:
//  1. A [CheckpointError] with a known scope. The checkpoint path already
//     classified the failure, so its verdict is used as is.
//  2. A [ClientError]. The client stated the scope directly; see
//     [ClientError] for how an unknown scope value is read.
//
// A CheckpointError without a scope, such as one rebuilt by
// [ErrorFromObject], states nothing and is skipped. When nothing in the
// chain states a scope, unclassified is returned. The caller supplies it
// because the safe default differs by site: an error the handler returned
// is an ordinary failure of the execution, while an error from a client
// call outside the handler is presumed transient.
func failureScope(err error, unclassified ErrorScope) ErrorScope {
	var ce *CheckpointError
	if errors.As(err, &ce) {
		if s := ce.Scope(); s == ErrorScopeInvocation || s == ErrorScopeExecution {
			return s
		}
	}

	var clientErr *ClientError
	if errors.As(err, &clientErr) {
		return clientErr.effectiveScope()
	}

	return unclassified
}
