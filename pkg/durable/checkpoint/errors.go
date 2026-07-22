package checkpoint

import (
	"errors"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// -----------------------------------------------------------------------
// Checkpoint API error classification (docs/remaining-work.md §4 task 12)
// -----------------------------------------------------------------------
//
// # Design
//
// The JS SDK's classifyCheckpointError distinguishes unrecoverable 4xx
// errors (bad request shape, validation failures - retrying is pointless,
// the request will never succeed) from retryable errors (5xx server
// faults, throttling, transient network failures - retrying is the
// correct response). This file ports that same two-way classification for
// the Go SDK's checkpoint.Manager, using a small, package-local error type
// (CheckpointError) rather than reusing pkg/durable/operations'
// OperationError hierarchy.
//
// That is a deliberate package-boundary decision, not an oversight: this
// SDK's import graph already has operations depending on checkpoint
// (every operation file calls c.Checkpoint().Enqueue(...) - see step.go,
// wait.go, etc.), via dcontext.Context's Checkpoint() accessor. Importing
// pkg/durable/operations from pkg/durable/checkpoint to reuse
// OperationError would create an import cycle (operations -> checkpoint
// -> operations) and even setting that aside, would be backwards: a
// checkpoint API call failing is a lower-level, transport/backend concern
// than any specific durable OPERATION (Step/Wait/Callback/etc.) - the
// same checkpoint failure can happen underneath any operation type
// indiscriminately, so classifying it in terms of "which operation kind
// failed" (OperationError's Kind field) doesn't fit; classifying it in
// terms of "was this the backend rejecting the request outright, or a
// transient condition worth retrying" is the right, and different, axis
// for this layer. CheckpointError is therefore a small, self-contained
// analog of OperationError's embedding pattern (a common base +
// errors.As-discoverable concrete subtypes), scoped to exactly this
// package, with no cross-package type sharing in either direction.
//
// # What this SDK can actually observe about a live Checkpoint error
//
// Classification is based on the REAL error shapes the AWS SDK for Go v2's
// generated Lambda client can produce, confirmed by reading
// github.com/aws/aws-sdk-go-v2/service/lambda@v1.99.0/types/errors.go and
// github.com/aws/smithy-go@v1.27.3/errors.go directly (not guessed):
//
//   - Every one of the ~50 generated exception types in the Lambda
//     service's types package (ValidationException is notably NOT among
//     them - see below) implements smithy.APIError, which exposes
//     ErrorCode() (the exception's wire name, e.g.
//     "InvalidParameterValueException"), ErrorMessage(), and critically
//     ErrorFault() - returning either smithy.FaultClient or
//     smithy.FaultServer, a fault classification the SDK's own generated
//     code assigns per-exception-type at codegen time from the service's
//     Smithy model (not inferred from the HTTP status code by this SDK -
//     it is the AWS SDK for Go v2's own authoritative classification).
//     FaultClient meant "this request was malformed/invalid, retrying the
//     exact same request will never succeed" for every 4xx exception
//     checked (InvalidParameterValueException, InvalidRequestContentException,
//     ResourceNotFoundException, RequestTooLargeException, etc.) - EXCEPT
//     one confirmed exception: TooManyRequestsException (HTTP 429
//     throttling) is ALSO tagged FaultClient in the generated code, despite
//     being the canonical "please retry, just not immediately" case in
//     every AWS SDK. Naively treating "FaultClient => non-retryable" would
//     therefore misclassify throttling - the single most important
//     retryable condition to get right - as unrecoverable. This is handled
//     explicitly below via an ErrorCode-based exception list, not a blanket
//     fault-based rule.
//   - FaultServer (ServiceException - "Lambda service encountered an
//     internal error", EC2ThrottledException, ENINotReadyException,
//     ResourceNotReadyException, and others) is treated as retryable
//     across the board - a 5xx-equivalent server-side condition is exactly
//     what this task's own instructions describe as retryable, and no
//     FaultServer exception in the generated list represents a
//     request-shape problem that re-sending would fail to fix.
//   - The real backend's ValidationException, independently CONFIRMED to
//     exist via a live smoke test against the actual Checkpoint endpoint
//     (see docs/remaining-work.md task 20's writeup: "received a real,
//     correctly-parsed ValidationException back"), is NOT one of the
//     Lambda service's own generated/modeled exception types (grepped
//     types/errors.go directly - there is no ValidationException type in
//     that file). This means a live ValidationException response
//     deserializes through smithy-go's generic, UNMODELED-error fallback
//     path as *smithy.GenericAPIError{Code: "ValidationException", Fault:
//     ...} rather than a named Go type - still fully handled here, since
//     classification below dispatches on smithy.APIError (the interface
//     both modeled AND GenericAPIError implement) and its ErrorCode()/
//     ErrorFault(), not on a concrete named type. GenericAPIError's own
//     Fault is populated by smithy-go's HTTP deserializer from the
//     response's HTTP status code for unmodeled errors (4xx => FaultClient,
//     5xx => FaultServer per smithy-go's own deserialization convention) -
//     so an unmodeled 4xx like the observed ValidationException still
//     correctly classifies as non-retryable via the same FaultClient path.
//   - Failing that (no smithy.APIError in the chain at all - a raw
//     transport-level failure that never got far enough to deserialize a
//     structured API error), smithyhttp.ResponseError - exposing the raw
//     HTTPStatusCode() - is checked as a second-tier fallback: >=500 is
//     retryable, 4xx is not, matching the exact same 4xx-vs-5xx split the
//     task's own instructions specify, for the case where a genuine HTTP
//     response was received but wasn't a recognized/deserializable AWS
//     error shape.
//   - If NEITHER is present - a plain network error (connection refused,
//     DNS failure, TLS handshake failure) or context.DeadlineExceeded/
//     context.Canceled from a client-side timeout - there is no
//     structured signal to classify against at all. This is the one
//     genuinely UNCERTAIN case this task's own instructions asked to be
//     flagged rather than asserted as fact: see classifyCheckpointError's
//     default branch below for why this SDK conservatively treats an
//     unrecognized error as retryable (a transient network blip is a far
//     more common real-world cause of an unstructured error than a
//     permanent one, and retrying a network-level failure is always safe
///    - it either succeeds, or fails again the same way, and the retry
//     loop's own bounded attempt count (see sendBatch's maxCheckpointAttempts)
//     caps the cost either way).
//
// # What is verified vs. genuinely uncertain
//
// VERIFIED (read directly from the pinned aws-sdk-go-v2/service/lambda and
// smithy-go source in this repo's module cache, not asserted from memory):
//   - The exact ErrorFault() value smithy-go's generated code assigns to
//     every one of the Lambda service's ~50 modeled exception types,
//     including that TooManyRequestsException is FaultClient despite being
//     the throttling case.
//   - That ValidationException is not a modeled type in this service
//     package, consistent with the live smoke test in task 20's writeup
//     observing it as a raw, presumably-unmodeled 4xx.
//   - smithy.APIError's and smithyhttp.ResponseError's method signatures
//     and Unwrap chains, confirming errors.As can recover either from
//     underneath the fmt.Errorf("...: %w", err) wrapping
//     awssdk.Client.Checkpoint already applies (see that method's own
//     "awssdk.Client.Checkpoint: %w" wrap - errors.As walks through it).
//
// NOT independently verified, and NOT asserted as fact - flagged here and
// in classifyCheckpointError's own doc:
//   - Whether TooManyRequestsException's RetryAfterSeconds field (present
//     on the modeled type, confirmed in types/errors.go) is reliably
//     populated by the real backend for a THROTTLED CheckpointDurableExecution
//     call specifically, as opposed to being populated for some other
//     Lambda API's throttling response only. This SDK does NOT currently
//     read RetryAfterSeconds to size its retry backoff (see
//     manager.go's checkpointRetryBaseDelay/checkpointRetryMaxDelay
//     doc) - it uses a fixed, conservative exponential backoff instead,
//     specifically BECAUSE this field's real population behavior for this
//     specific API was not confirmed and a live throttling scenario was
//     never triggered/observed in this session (no live smoke test against
//     the real Checkpoint endpoint was performed as part of this task -
//     see this task's final report for what live verification WAS vs.
//     was NOT performed).
//   - Whether the real backend ever returns a 429/throttling response for
//     Checkpoint at all in practice (as opposed to TooManyRequestsException
//     merely existing as a modeled possibility on the Lambda service as a
//     whole) - not confirmed either way; classified as retryable anyway
//     since that is the safe default for a throttling-shaped error even
//     if it turns out to be rare in practice for this specific API.
//   - Whether a raw network-level error (no HTTP response at all) can
//     genuinely occur for a real Checkpoint call in a Lambda execution
//     environment's networking conditions - presumed possible (as it is
//     for any network call) but not specifically observed.

// CheckpointErrorKind classifies why a checkpoint.Client.Checkpoint call
// failed, for callers that want to branch on retryability without a type
// switch. See classifyCheckpointError for the exact rules.
type CheckpointErrorKind string

const (
	// CheckpointErrorKindRetryable marks an error the caller should retry
	// (server fault, throttling, or an unrecognized/network-level error -
	// see this file's top-level doc for the "unrecognized defaults to
	// retryable" rationale).
	CheckpointErrorKindRetryable CheckpointErrorKind = "RETRYABLE"
	// CheckpointErrorKindNonRetryable marks an error where retrying the
	// exact same request is expected to fail again identically (a 4xx
	// client-fault response other than throttling) - matching JS's
	// unrecoverable-error classification.
	CheckpointErrorKindNonRetryable CheckpointErrorKind = "NON_RETRYABLE"
)

// CheckpointError is the common base for this package's classified
// checkpoint-API errors - the checkpoint-package-local analog of
// pkg/durable/operations.OperationError (see this file's top-level doc for
// why this is a separate, non-shared type rather than a reused import).
//
// Every checkpoint.Client.Checkpoint failure that flows through
// classifyCheckpointError is wrapped as either a *RetryableCheckpointError
// or a *NonRetryableCheckpointError, both of which embed *CheckpointError,
// so a caller can recover the common classification via:
//
//	var ckptErr *checkpoint.CheckpointError
//	if errors.As(err, &ckptErr) {
//		log.Printf("checkpoint call failed (kind=%s): %v", ckptErr.Kind, ckptErr.Err)
//	}
//
// or check the specific subtype directly (errors.As(err, &retryableErr))
// when the caller only wants ONE of the two branches. The original error
// (whatever classifyCheckpointError was given - e.g. an
// *awssdk.Client.Checkpoint-wrapped smithy.APIError) is always preserved,
// unwrappable, as Err.
type CheckpointError struct {
	Kind CheckpointErrorKind
	// Err is the original, unclassified error returned by
	// checkpoint.Client.Checkpoint (see Manager.sendBatch).
	Err error
}

func (e *CheckpointError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err != nil {
		return "checkpoint API error (" + string(e.Kind) + "): " + e.Err.Error()
	}
	return "checkpoint API error (" + string(e.Kind) + ")"
}

// Unwrap exposes the original cause to errors.Is/errors.As chains (e.g. so
// a caller can still errors.As all the way down to the underlying
// smithy.APIError/lambdatypes.TooManyRequestsException, etc., through this
// wrapping).
func (e *CheckpointError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// RetryableCheckpointError marks a checkpoint API failure the caller
// should retry - a server fault (5xx-equivalent), a throttling response,
// or an error this SDK could not confidently classify at all (see this
// file's top-level doc's "NOT independently verified" section for the
// honesty caveat on that last case).
type RetryableCheckpointError struct {
	*CheckpointError
}

// Unwrap returns the embedded *CheckpointError base itself (not just its
// own Err), exactly mirroring pkg/durable/operations/errors.go's identical
// StepFailedError.Unwrap override and for the identical reason: Go's
// struct embedding promotes CheckpointError's own Unwrap() (which returns
// Err, the deeper cause) directly onto RetryableCheckpointError, so a
// naive errors.As(err, &ckptErr) would skip past the *CheckpointError base
// entirely and go straight to its cause, never matching *CheckpointError
// at all. This override restores the base as the thing errors.As finds
// one unwrap step before it would otherwise reach Err.
func (e *RetryableCheckpointError) Unwrap() error { return e.CheckpointError }

// NonRetryableCheckpointError marks a checkpoint API failure the caller
// should NOT retry - a 4xx-equivalent client-fault response (other than
// throttling) indicating the request itself was invalid and will fail
// identically on any retry, matching JS's isUnrecoverableError semantics.
type NonRetryableCheckpointError struct {
	*CheckpointError
}

// Unwrap - see RetryableCheckpointError.Unwrap's doc for why this override
// is required.
func (e *NonRetryableCheckpointError) Unwrap() error { return e.CheckpointError }

// throttlingErrorCodes lists the modeled Lambda exception ErrorCode()
// values that represent a throttling/rate-limit condition despite being
// tagged smithy.FaultClient by the generated SDK code (see this file's
// top-level doc). Confirmed by reading
// aws-sdk-go-v2/service/lambda@v1.99.0/types/errors.go directly:
// TooManyRequestsException is the only such exception defined on this
// service today. Kept as a slice (rather than a single constant) so a
// future SDK version adding another throttling-shaped exception has an
// obvious place to add it, with this same explanatory comment as
// precedent.
var throttlingErrorCodes = map[string]bool{
	"TooManyRequestsException": true,
}

// classifyCheckpointError inspects err (as returned by
// checkpoint.Client.Checkpoint - see Manager.sendBatch, the sole call
// site) and wraps it as either a *RetryableCheckpointError or a
// *NonRetryableCheckpointError, matching the JS SDK's
// classifyCheckpointError (docs/remaining-work.md §4 task 12). See this
// file's top-level doc for the full rationale and the exact, source-
// verified error shapes this is based on. Returns nil if err is nil.
//
// Classification order (first match wins):
//
//  1. A smithy.APIError anywhere in err's chain (covers both a named,
//     modeled exception type like *lambdatypes.InvalidParameterValueException
//     AND the generic *smithy.GenericAPIError fallback smithy-go uses for
//     unmodeled errors like the real backend's own ValidationException -
//     both implement this same interface):
//     - ErrorCode() is checked against throttlingErrorCodes FIRST, since
//     throttling is FaultClient despite being retryable (see this
//     file's top doc) - this check must come before the Fault check
//     below, not after, or throttling would be misclassified.
//     - Otherwise, ErrorFault() decides: FaultServer => retryable,
//     FaultClient => non-retryable.
//  2. A smithyhttp.ResponseError (a real HTTP response was received, but
//     it didn't deserialize into a recognized API error shape) - its
//     HTTPStatusCode() decides: >=500 => retryable, else (4xx) =>
//     non-retryable.
//  3. Anything else (including plain network/context errors) - retryable
//     by default. This is the ONE genuinely uncertain branch - see this
//     file's top-level doc's honesty section. It is deliberately
//     conservative: an unrecognized error is far more likely to be a
//     transient condition (network blip, DNS hiccup, client-side
//     timeout) than a permanently-unrecoverable one, and the cost of
//     wrongly retrying a truly permanent-but-unrecognized failure is
//     bounded by sendBatch's own fixed maxCheckpointAttempts, whereas the
//     cost of wrongly giving up on a transient failure is an entire
//     durable execution failing outright for no real reason.
func classifyCheckpointError(err error) error {
	if err == nil {
		return nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if throttlingErrorCodes[apiErr.ErrorCode()] {
			return &RetryableCheckpointError{&CheckpointError{Kind: CheckpointErrorKindRetryable, Err: err}}
		}
		if apiErr.ErrorFault() == smithy.FaultServer {
			return &RetryableCheckpointError{&CheckpointError{Kind: CheckpointErrorKindRetryable, Err: err}}
		}
		return &NonRetryableCheckpointError{&CheckpointError{Kind: CheckpointErrorKindNonRetryable, Err: err}}
	}

	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		if respErr.HTTPStatusCode() >= 500 {
			return &RetryableCheckpointError{&CheckpointError{Kind: CheckpointErrorKindRetryable, Err: err}}
		}
		return &NonRetryableCheckpointError{&CheckpointError{Kind: CheckpointErrorKindNonRetryable, Err: err}}
	}

	// Unrecognized shape (e.g. a raw network error, context deadline, or
	// checkpoint.Client.Checkpoint's own local nil-API-style Go error) -
	// see this function's doc, point 3, for why this defaults to
	// retryable rather than non-retryable.
	return &RetryableCheckpointError{&CheckpointError{Kind: CheckpointErrorKindRetryable, Err: err}}
}

// IsRetryable reports whether err (as returned by classifyCheckpointError,
// or anything wrapping such an error) should be retried. Exported so
// callers outside this package (tests, or a future caller inspecting an
// Enqueue failure) can check retryability without needing to know about
// the two concrete subtypes.
func IsRetryable(err error) bool {
	var retryable *RetryableCheckpointError
	return errors.As(err, &retryable)
}
