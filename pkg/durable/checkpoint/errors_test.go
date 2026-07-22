package checkpoint

import (
	"errors"
	"net/http"
	"testing"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestClassifyCheckpointError_Nil(t *testing.T) {
	if err := classifyCheckpointError(nil); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestClassifyCheckpointError_ModeledClientFault_NonRetryable(t *testing.T) {
	// InvalidParameterValueException-shaped: a modeled 4xx exception
	// tagged smithy.FaultClient - the canonical "bad request, don't
	// retry" case (see errors.go's top-level doc).
	err := &smithy.GenericAPIError{Code: "InvalidParameterValueException", Message: "bad token", Fault: smithy.FaultClient}
	classified := classifyCheckpointError(err)

	if IsRetryable(classified) {
		t.Fatalf("expected non-retryable, got retryable: %v", classified)
	}
	var nonRetryable *NonRetryableCheckpointError
	if !errors.As(classified, &nonRetryable) {
		t.Fatalf("expected *NonRetryableCheckpointError, got %T", classified)
	}
	var ckptErr *CheckpointError
	if !errors.As(classified, &ckptErr) {
		t.Fatalf("expected errors.As to recover *CheckpointError base, got none")
	}
	if ckptErr.Kind != CheckpointErrorKindNonRetryable {
		t.Errorf("unexpected Kind: %s", ckptErr.Kind)
	}
	// The original error must still be reachable via Unwrap.
	var original *smithy.GenericAPIError
	if !errors.As(classified, &original) {
		t.Fatalf("expected errors.As to recover the original *smithy.GenericAPIError cause")
	}
}

func TestClassifyCheckpointError_UnmodeledValidationException_NonRetryable(t *testing.T) {
	// The real backend's ValidationException, per docs/remaining-work.md
	// task 20's live smoke test, is NOT a modeled exception type on this
	// service (confirmed by reading types/errors.go - see errors.go's
	// doc) - it deserializes via smithy-go's GenericAPIError fallback.
	// This test specifically exercises that unmodeled path, not a named
	// exception type, to guard against a classification that only works
	// for types this repo happens to have a Go struct for.
	err := &smithy.GenericAPIError{Code: "ValidationException", Message: "Invalid ARN", Fault: smithy.FaultClient}
	classified := classifyCheckpointError(err)

	if IsRetryable(classified) {
		t.Fatalf("expected non-retryable, got retryable: %v", classified)
	}
}

func TestClassifyCheckpointError_ServerFault_Retryable(t *testing.T) {
	// ServiceException-shaped: "Lambda service encountered an internal
	// error" - a modeled 5xx-equivalent exception tagged
	// smithy.FaultServer.
	err := &smithy.GenericAPIError{Code: "ServiceException", Message: "internal error", Fault: smithy.FaultServer}
	classified := classifyCheckpointError(err)

	if !IsRetryable(classified) {
		t.Fatalf("expected retryable, got non-retryable: %v", classified)
	}
	var retryable *RetryableCheckpointError
	if !errors.As(classified, &retryable) {
		t.Fatalf("expected *RetryableCheckpointError, got %T", classified)
	}
}

func TestClassifyCheckpointError_Throttling_RetryableDespiteClientFault(t *testing.T) {
	// TooManyRequestsException is tagged smithy.FaultClient by the
	// generated SDK code (confirmed in
	// aws-sdk-go-v2/service/lambda@v1.99.0/types/errors.go) despite being
	// the canonical retryable throttling case - this is the exact
	// misclassification this task's own instructions warned against, and
	// the reason classifyCheckpointError checks ErrorCode() against
	// throttlingErrorCodes BEFORE falling back to the generic
	// Fault-based rule.
	err := &smithy.GenericAPIError{Code: "TooManyRequestsException", Message: "rate exceeded", Fault: smithy.FaultClient}
	classified := classifyCheckpointError(err)

	if !IsRetryable(classified) {
		t.Fatalf("expected throttling to classify as retryable despite FaultClient, got non-retryable: %v", classified)
	}
}

func TestClassifyCheckpointError_ResponseError_5xx_Retryable(t *testing.T) {
	respErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 503}},
		Err:      errors.New("service unavailable"),
	}
	classified := classifyCheckpointError(respErr)

	if !IsRetryable(classified) {
		t.Fatalf("expected 503 ResponseError to be retryable, got: %v", classified)
	}
}

func TestClassifyCheckpointError_ResponseError_4xx_NonRetryable(t *testing.T) {
	respErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 400}},
		Err:      errors.New("bad request"),
	}
	classified := classifyCheckpointError(respErr)

	if IsRetryable(classified) {
		t.Fatalf("expected 400 ResponseError to be non-retryable, got: %v", classified)
	}
}

func TestClassifyCheckpointError_UnrecognizedError_DefaultsRetryable(t *testing.T) {
	// A plain network-shaped error with no smithy.APIError or
	// smithyhttp.ResponseError in its chain at all - the genuinely
	// uncertain case documented at length in errors.go's top-level doc.
	// This SDK conservatively defaults this to retryable.
	err := errors.New("dial tcp: connection refused")
	classified := classifyCheckpointError(err)

	if !IsRetryable(classified) {
		t.Fatalf("expected unrecognized error to default to retryable, got: %v", classified)
	}
}

func TestClassifyCheckpointError_WrappedAPIError_StillClassified(t *testing.T) {
	// Mirrors how awssdk.Client.Checkpoint actually returns errors -
	// wrapped via fmt.Errorf("awssdk.Client.Checkpoint: %w", err) - to
	// confirm errors.As sees through that wrapping layer, exactly as
	// this file's doc claims.
	inner := &smithy.GenericAPIError{Code: "ServiceException", Fault: smithy.FaultServer}
	wrapped := &wrapError{msg: "awssdk.Client.Checkpoint: service error", err: inner}

	classified := classifyCheckpointError(wrapped)
	if !IsRetryable(classified) {
		t.Fatalf("expected wrapped server-fault error to be retryable, got: %v", classified)
	}
}

// wrapError is a minimal %w-style wrapper, avoiding a fmt.Errorf import
// purely for this one test.
type wrapError struct {
	msg string
	err error
}

func (w *wrapError) Error() string { return w.msg }
func (w *wrapError) Unwrap() error { return w.err }
