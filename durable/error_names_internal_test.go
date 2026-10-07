package durable

import (
	"errors"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// TestNonDeterminismNameOnlyMismatchFields asserts that a name-only replay
// mismatch reports the checkpointed name in RecordedName and the name the
// handler now runs in CurrentName.
func TestNonDeterminismNameOnlyMismatchFields(t *testing.T) {
	op := &operation{id: "c4ca4238a0b92382", opType: "STEP", subType: "Step", name: "a"}
	err := validateReplayConsistency(op, "STEP", "Step", "b")
	var nd *NonDeterministicExecutionError
	if !errors.As(err, &nd) {
		t.Fatalf("err = %v (%T), want *NonDeterministicExecutionError", err, err)
	}
	if nd.RecordedName != "a" || nd.CurrentName != "b" {
		t.Errorf("RecordedName/CurrentName = %q/%q, want a/b", nd.RecordedName, nd.CurrentName)
	}
	if nd.RecordedType != "STEP" || nd.CurrentType != "STEP" || nd.RecordedSubType != "Step" || nd.CurrentSubType != "Step" {
		t.Errorf("types = %+v, want STEP/Step on both sides", nd)
	}
	want := `durable: non-deterministic replay at step "c4ca4238a0b92382": ` +
		`the checkpoint recorded operation (type STEP, subtype "Step", name "a"), ` +
		`but the handler now runs (type STEP, subtype "Step", name "b"). ` +
		`The handler code changed between deployments.`
	if got := err.Error(); got != want {
		t.Errorf("Error() =\n%s\nwant\n%s", got, want)
	}
}

// TestNonDeterminismWireName asserts the recorded wire name of a replay
// mismatch and that the conformance pattern for non-determinism matches it.
func TestNonDeterminismWireName(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"zero value", &NonDeterministicExecutionError{}},
		{"type mismatch", &NonDeterministicExecutionError{Name: "a", StepID: "1", CurrentType: "WAIT", RecordedType: "STEP"}},
		{"name mismatch", &NonDeterministicExecutionError{Name: "b", StepID: "1", CurrentName: "b", RecordedName: "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := sdkWireErrorType(tc.err)
			if !ok || got != "NonDeterministicExecutionError" {
				t.Errorf("sdkWireErrorType = %q, %v; want NonDeterministicExecutionError, true", got, ok)
			}
			if wire := wireErrorType(tc.err); wire != "NonDeterministicExecutionError" {
				t.Errorf("wireErrorType = %q, want NonDeterministicExecutionError", wire)
			}
		})
	}
	if !regexp.MustCompile(`(?i).*non[-_ ]?determin.*`).MatchString("NonDeterministicExecutionError") {
		t.Error("the conformance pattern does not match NonDeterministicExecutionError")
	}
}

// TestErrorFromObjectNonDeterministicExecutionError asserts that the wire
// name rebuilds the Go type.
func TestErrorFromObjectNonDeterministicExecutionError(t *testing.T) {
	err := ErrorFromObject(&ErrorObject{ErrorType: aws.String("NonDeterministicExecutionError"), ErrorMessage: aws.String("m")})
	var nd *NonDeterministicExecutionError
	if !errors.As(err, &nd) {
		t.Fatalf("ErrorFromObject = %v (%T), want *NonDeterministicExecutionError", err, err)
	}
	if err.Error() != "m" {
		t.Errorf("Error() = %q, want the recorded message", err.Error())
	}
}

// TestBatchCompletionErrorMatchesOperationError asserts that errors.As
// against *OperationError matches a *BatchCompletionError, and that a
// rebuilt value keeps its type and reason.
func TestBatchCompletionErrorMatchesOperationError(t *testing.T) {
	err := error(&BatchCompletionError{Name: "m", Reason: CompletionCustomFailed})
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatal("errors.As(*BatchCompletionError, *OperationError) = false, want true")
	}
	if opErr.Name != "m" || opErr.Message != err.Error() {
		t.Errorf("OperationError = %+v, want name m and the error's message", opErr)
	}
	if got, _ := sdkWireErrorType(err); got != "BatchCompletionError" {
		t.Errorf("sdkWireErrorType = %q, want BatchCompletionError", got)
	}
	rebuilt := ErrorFromObject(&ErrorObject{ErrorType: aws.String("BatchCompletionError"), ErrorMessage: aws.String(err.Error())})
	var cerr *BatchCompletionError
	if !errors.As(rebuilt, &cerr) || cerr.Reason != CompletionCustomFailed || rebuilt.Error() != err.Error() {
		t.Errorf("ErrorFromObject = %v (%T), want *BatchCompletionError with CUSTOM_COMPLETION_FAILED", rebuilt, rebuilt)
	}
}
