package durable

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// publicErrorTypeSamples returns one live value of every public error type
// the SDK records, built the way the SDK builds it. It is the fixture for
// the ErrorFromObject round-trip tests.
func publicErrorTypeSamples() []error {
	cause := errorRecord{errType: "Error", message: "boom"}
	return []error{
		newStepError("s", 3, cause),
		&StepInterruptedError{Name: "s"},
		&InvokeError{Name: "i", FunctionID: "fn", Status: OperationStatusFailed, ErrorType: "Error", Message: "boom", Err: cause.standIn(nil)},
		&CallbackError{Name: "c", CallbackID: "cb", ErrorType: "Error", Message: "boom", Err: cause.standIn(nil)},
		newCallbackExternalError("c", "cb", errorRecord{errType: "VendorError", message: "declined"}),
		newCallbackTimeoutError("c", "cb", errorRecord{}),
		newCallbackSubmitterError("c", "cb", cause),
		newChildContextError("child", cause),
		&WaitForConditionError{Name: "w", Attempts: 2, ErrorType: "Error", Message: "boom", Err: cause.standIn(nil)},
		&CombinatorError{Name: "any", Errors: []error{errors.New("a"), errors.New("b")}},
		&BatchCompletionError{Reason: CompletionFailureToleranceExceeded},
		&OperationError{Name: "op", ErrorType: "Error", Message: "boom", Err: cause.standIn(nil)},
		&NonDeterministicReplayError{Name: "n", StepID: "1", ExpectedType: "STEP", ActualType: "WAIT"},
		&ResultTooLargeError{Name: "r", SizeBytes: 900, LimitBytes: 100},
		newSerdesError("s", serdesDirectionMarshal, errors.New("bad json")),
		&CheckpointError{Err: errors.New("throttled"), retryable: true},
	}
}

// TestErrorFromObjectCoversEverySDKWireName asserts that the fixture holds
// one value for every wire name [sdkWireErrorType] can produce, so the
// round-trip tests cannot silently skip a type.
func TestErrorFromObjectCoversEverySDKWireName(t *testing.T) {
	seen := map[string]bool{}
	for _, err := range publicErrorTypeSamples() {
		name, ok := sdkWireErrorType(err)
		if !ok {
			t.Fatalf("%T is not an SDK error type", err)
		}
		seen[name] = true
	}
	for name := range sdkErrorsByWireType {
		if name == legacyCallbackTimeoutType || name == legacyCallbackHeartbeatType {
			continue
		}
		if !seen[name] {
			t.Errorf("no fixture for wire name %q", name)
		}
	}
	if name, _ := sdkWireErrorType(&CheckpointError{}); !seen[name] {
		t.Errorf("no fixture for wire name %q", name)
	}
}

// TestErrorFromObjectRoundTrip serializes every public error type the way
// the SDK records a failure, rebuilds it with ErrorFromObject, and asserts
// that the type and the message survive: errors.As matches the original
// concrete type and *OperationError, the OperationError's Message is the
// recorded message, and the rebuilt Error() text carries it.
func TestErrorFromObjectRoundTrip(t *testing.T) {
	for _, orig := range publicErrorTypeSamples() {
		wireName, _ := sdkWireErrorType(orig)
		t.Run(wireName, func(t *testing.T) {
			obj := errorObject(orig)
			if got := aws.ToString(obj.ErrorType); got != wireName {
				t.Fatalf("recorded ErrorType = %q, want %q", got, wireName)
			}
			recorded := aws.ToString(obj.ErrorMessage)

			rebuilt := ErrorFromObject(obj)
			if rebuilt == nil {
				t.Fatal("ErrorFromObject returned nil")
			}

			// errors.As against the original concrete type.
			target := reflect.New(reflect.TypeOf(orig))
			if !errors.As(rebuilt, target.Interface()) {
				t.Errorf("errors.As(%T) = false for rebuilt %T", orig, rebuilt)
			}

			var opErr *OperationError
			if !errors.As(rebuilt, &opErr) {
				t.Fatalf("errors.As(*OperationError) = false for rebuilt %T", rebuilt)
			}
			if opErr.Message != recorded {
				t.Errorf("OperationError.Message = %q, want recorded %q", opErr.Message, recorded)
			}
			if !strings.Contains(rebuilt.Error(), recorded) {
				t.Errorf("rebuilt Error() = %q, does not contain recorded message %q", rebuilt.Error(), recorded)
			}

			// A type that is an operation error is rebuilt as itself, so
			// recording it again yields the same wire name.
			if _, isOp := orig.(interface{ operationError() *OperationError }); isOp {
				if got := wireErrorType(rebuilt); got != wireName {
					t.Errorf("wireErrorType(rebuilt) = %q, want %q", got, wireName)
				}
				if reflect.TypeOf(rebuilt) != reflect.TypeOf(orig) {
					t.Errorf("rebuilt is %T, want %T", rebuilt, orig)
				}
			}
		})
	}
}

// TestErrorFromObjectKeepsRecordedMessageAsErrorText asserts that the types
// whose Error() text is composed from detail fields the record does not
// carry report the recorded message verbatim once rebuilt.
func TestErrorFromObjectKeepsRecordedMessageAsErrorText(t *testing.T) {
	for _, orig := range []error{
		&StepInterruptedError{Name: "s"},
		&CombinatorError{Name: "any", Errors: []error{errors.New("a"), errors.New("b")}},
		&BatchCompletionError{Reason: CompletionMinSuccessfulReached},
		&NonDeterministicReplayError{Name: "n", StepID: "1", ExpectedType: "STEP", ActualType: "WAIT"},
		&ResultTooLargeError{Name: "r", SizeBytes: 900, LimitBytes: 100},
	} {
		rebuilt := ErrorFromObject(errorObject(orig))
		if rebuilt.Error() != orig.Error() {
			t.Errorf("%T: rebuilt Error() = %q, want %q", orig, rebuilt.Error(), orig.Error())
		}
	}
}

// TestErrorFromObjectCarriesRecordFields asserts that ErrorData and the
// stack trace are carried into the typed error and into the OperationError
// reached through it.
func TestErrorFromObjectCarriesRecordFields(t *testing.T) {
	obj := &ErrorObject{
		ErrorType:    aws.String("StepError"),
		ErrorMessage: aws.String("step failed"),
		ErrorData:    aws.String(`{"code":7}`),
		StackTrace:   []string{"frame 1", "frame 2"},
	}
	rebuilt := ErrorFromObject(obj)

	var stepErr *StepError
	if !errors.As(rebuilt, &stepErr) {
		t.Fatalf("errors.As(*StepError) = false for %T", rebuilt)
	}
	if stepErr.ErrorData != `{"code":7}` {
		t.Errorf("ErrorData = %q", stepErr.ErrorData)
	}
	if !reflect.DeepEqual(stepErr.StackTrace, []string{"frame 1", "frame 2"}) {
		t.Errorf("StackTrace = %v", stepErr.StackTrace)
	}
	if stepErr.Message != "step failed" {
		t.Errorf("Message = %q", stepErr.Message)
	}

	var opErr *OperationError
	if !errors.As(rebuilt, &opErr) {
		t.Fatal("errors.As(*OperationError) = false")
	}
	if opErr.ErrorData != stepErr.ErrorData || !reflect.DeepEqual(opErr.StackTrace, stepErr.StackTrace) {
		t.Errorf("OperationError fields differ: %+v", opErr)
	}
}

// TestErrorFromObjectUnknownNameFallsBack asserts the documented fallback:
// a name the SDK does not write yields an *OperationError carrying the
// record, never an error or a panic.
func TestErrorFromObjectUnknownNameFallsBack(t *testing.T) {
	tests := []struct {
		name         string
		obj          *ErrorObject
		wantErrType  string
		wantMessage  string
		wantCauseNil bool
		wantCause    string
	}{
		{
			name:        "handler error type",
			obj:         &ErrorObject{ErrorType: aws.String("PaymentDeclinedError"), ErrorMessage: aws.String("card declined")},
			wantErrType: "PaymentDeclinedError",
			wantMessage: "card declined",
			wantCause:   "PaymentDeclinedError: card declined",
		},
		{
			name:        "unnamed error",
			obj:         &ErrorObject{ErrorType: aws.String("Error"), ErrorMessage: aws.String("boom")},
			wantErrType: "Error",
			wantMessage: "boom",
			wantCause:   "Error: boom",
		},
		{
			name:        "empty type with message",
			obj:         &ErrorObject{ErrorMessage: aws.String("boom")},
			wantMessage: "boom",
			wantCause:   "boom",
		},
		{
			name:         "empty record",
			obj:          &ErrorObject{},
			wantCauseNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rebuilt := ErrorFromObject(tt.obj)
			opErr, ok := rebuilt.(*OperationError)
			if !ok {
				t.Fatalf("got %T, want *OperationError", rebuilt)
			}
			if opErr.ErrorType != tt.wantErrType || opErr.Message != tt.wantMessage {
				t.Errorf("ErrorType/Message = %q/%q, want %q/%q", opErr.ErrorType, opErr.Message, tt.wantErrType, tt.wantMessage)
			}
			if tt.wantCauseNil {
				if opErr.Err != nil {
					t.Errorf("Err = %v, want nil", opErr.Err)
				}
				return
			}
			if opErr.Err == nil || opErr.Err.Error() != tt.wantCause {
				t.Errorf("Err = %v, want %q", opErr.Err, tt.wantCause)
			}
			var stepErr *StepError
			if errors.As(rebuilt, &stepErr) {
				t.Error("errors.As(*StepError) = true for an unknown name")
			}
		})
	}
}

// TestErrorFromObjectNil asserts that a nil record yields nil.
func TestErrorFromObjectNil(t *testing.T) {
	if err := ErrorFromObject(nil); err != nil {
		t.Errorf("ErrorFromObject(nil) = %v, want nil", err)
	}
}

// TestErrorFromObjectCallbackTimeouts asserts that a callback timeout
// record, under the current name or an older one, is rebuilt as a
// *CallbackTimeoutError that unwraps to ErrCallbackTimedOut, with Heartbeat
// derived from the record.
func TestErrorFromObjectCallbackTimeouts(t *testing.T) {
	tests := []struct {
		errType       string
		message       string
		wantHeartbeat bool
	}{
		{"CallbackTimeoutError", "Callback timed out", false},
		{"CallbackTimeoutError", "Callback heartbeat timed out", true},
		{legacyCallbackTimeoutType, "", false},
		{legacyCallbackHeartbeatType, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.errType+"/"+tt.message, func(t *testing.T) {
			obj := &ErrorObject{ErrorType: aws.String(tt.errType)}
			if tt.message != "" {
				obj.ErrorMessage = aws.String(tt.message)
			}
			rebuilt := ErrorFromObject(obj)

			var timeout *CallbackTimeoutError
			if !errors.As(rebuilt, &timeout) {
				t.Fatalf("got %T, want *CallbackTimeoutError", rebuilt)
			}
			if timeout.Heartbeat != tt.wantHeartbeat {
				t.Errorf("Heartbeat = %v, want %v", timeout.Heartbeat, tt.wantHeartbeat)
			}
			if !errors.Is(rebuilt, ErrCallbackTimedOut) {
				t.Error("errors.Is(ErrCallbackTimedOut) = false")
			}
			var cbErr *CallbackError
			if !errors.As(rebuilt, &cbErr) {
				t.Error("errors.As(*CallbackError) = false")
			}
			if timeout.ErrorType != tt.errType {
				t.Errorf("ErrorType = %q, want the recorded %q", timeout.ErrorType, tt.errType)
			}
		})
	}
}

// TestErrorFromObjectNonOperationTypesAreWrapped asserts that SerdesError
// and CheckpointError, which are not operation errors, come back inside an
// *OperationError so both matches hold.
func TestErrorFromObjectNonOperationTypesAreWrapped(t *testing.T) {
	t.Run("SerdesError", func(t *testing.T) {
		rebuilt := ErrorFromObject(&ErrorObject{ErrorType: aws.String("SerdesError"), ErrorMessage: aws.String("bad json")})
		if _, ok := rebuilt.(*OperationError); !ok {
			t.Fatalf("got %T, want *OperationError", rebuilt)
		}
		var serdesErr *SerdesError
		if !errors.As(rebuilt, &serdesErr) {
			t.Fatal("errors.As(*SerdesError) = false")
		}
		if !strings.Contains(serdesErr.Error(), "bad json") {
			t.Errorf("SerdesError.Error() = %q", serdesErr.Error())
		}
	})
	t.Run("CheckpointError", func(t *testing.T) {
		rebuilt := ErrorFromObject(&ErrorObject{ErrorType: aws.String("CheckpointError"), ErrorMessage: aws.String("throttled")})
		if _, ok := rebuilt.(*OperationError); !ok {
			t.Fatalf("got %T, want *OperationError", rebuilt)
		}
		var cpErr *CheckpointError
		if !errors.As(rebuilt, &cpErr) {
			t.Fatal("errors.As(*CheckpointError) = false")
		}
		if cpErr.Retryable() {
			t.Error("Retryable() = true for a rebuilt CheckpointError; the record carries no classification")
		}
		if !strings.Contains(cpErr.Error(), "throttled") {
			t.Errorf("CheckpointError.Error() = %q", cpErr.Error())
		}
	})
}
