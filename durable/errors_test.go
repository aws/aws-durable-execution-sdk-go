package durable

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorHierarchyIs(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target error
		wantIs bool
	}{
		{
			name:   "CallbackError wrapping ErrCallbackTimedOut",
			err:    &CallbackError{Name: "cb", Err: ErrCallbackTimedOut},
			target: ErrCallbackTimedOut,
			wantIs: true,
		},
		{
			name:   "InvokeError wrapping ErrExecutionStopped via replayedError",
			err:    &InvokeError{Name: "inv", Err: &replayedError{errType: "Error", message: "stopped", sentinel: ErrExecutionStopped}},
			target: ErrExecutionStopped,
			wantIs: true,
		},
		{
			name:   "InvokeError wrapping ErrExecutionCancelled via replayedError",
			err:    &InvokeError{Name: "inv", Err: &replayedError{errType: "Error", message: "cancelled", sentinel: ErrExecutionCancelled}},
			target: ErrExecutionCancelled,
			wantIs: true,
		},
		{
			name:   "InvokeError wrapping ErrInvokeTimedOut via replayedError",
			err:    &InvokeError{Name: "inv", Err: &replayedError{errType: "Error", message: "timed out", sentinel: ErrInvokeTimedOut}},
			target: ErrInvokeTimedOut,
			wantIs: true,
		},
		{
			name:   "plain StepError does not match sentinels",
			err:    &StepError{Name: "s", Attempts: 1, Err: fmt.Errorf("failed")},
			target: ErrCallbackTimedOut,
			wantIs: false,
		},
		{
			name:   "replayedError without sentinel does not match",
			err:    &replayedError{errType: "Err", message: "x"},
			target: ErrExecutionStopped,
			wantIs: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errors.Is(tt.err, tt.target); got != tt.wantIs {
				t.Errorf("errors.Is = %v, want %v", got, tt.wantIs)
			}
		})
	}
}

func TestErrorHierarchyAs(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target any // pointer to error type
		wantAs bool
	}{
		{"StepError", &StepError{Name: "s", Err: fmt.Errorf("x")}, new(*StepError), true},
		{"StepInterruptedError", &StepInterruptedError{Name: "s"}, new(*StepInterruptedError), true},
		{"InvokeError", &InvokeError{Name: "i", Err: fmt.Errorf("x")}, new(*InvokeError), true},
		{"CallbackError", &CallbackError{Name: "c", Err: fmt.Errorf("x")}, new(*CallbackError), true},
		{"ChildContextError", &ChildContextError{Name: "ch", Err: fmt.Errorf("x")}, new(*ChildContextError), true},
		{"WaitForConditionError", &WaitForConditionError{Name: "w", Attempts: 2, Err: fmt.Errorf("x")}, new(*WaitForConditionError), true},
		{"CombinatorError", &CombinatorError{Name: "c", Errors: []error{fmt.Errorf("x")}}, new(*CombinatorError), true},
		{"StepError not InvokeError", &StepError{Name: "s", Err: fmt.Errorf("x")}, new(*InvokeError), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch p := tt.target.(type) {
			case **StepError:
				if got := errors.As(tt.err, p); got != tt.wantAs {
					t.Errorf("errors.As = %v, want %v", got, tt.wantAs)
				}
			case **StepInterruptedError:
				if got := errors.As(tt.err, p); got != tt.wantAs {
					t.Errorf("errors.As = %v, want %v", got, tt.wantAs)
				}
			case **InvokeError:
				if got := errors.As(tt.err, p); got != tt.wantAs {
					t.Errorf("errors.As = %v, want %v", got, tt.wantAs)
				}
			case **CallbackError:
				if got := errors.As(tt.err, p); got != tt.wantAs {
					t.Errorf("errors.As = %v, want %v", got, tt.wantAs)
				}
			case **ChildContextError:
				if got := errors.As(tt.err, p); got != tt.wantAs {
					t.Errorf("errors.As = %v, want %v", got, tt.wantAs)
				}
			case **WaitForConditionError:
				if got := errors.As(tt.err, p); got != tt.wantAs {
					t.Errorf("errors.As = %v, want %v", got, tt.wantAs)
				}
			case **CombinatorError:
				if got := errors.As(tt.err, p); got != tt.wantAs {
					t.Errorf("errors.As = %v, want %v", got, tt.wantAs)
				}
			}
		})
	}
}

func TestErrorUnwrap(t *testing.T) {
	cause := fmt.Errorf("root cause")
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"StepError.Unwrap", &StepError{Err: cause}, cause},
		{"InvokeError.Unwrap", &InvokeError{Err: cause}, cause},
		{"CallbackError.Unwrap", &CallbackError{Err: cause}, cause},
		{"ChildContextError.Unwrap", &ChildContextError{Err: cause}, cause},
		{"WaitForConditionError.Unwrap", &WaitForConditionError{Err: cause}, cause},
		{"SerdesError.Unwrap", &SerdesError{Err: cause}, cause},
		{"replayedError.Unwrap nil", &replayedError{}, nil},
		{"replayedError.Unwrap sentinel", &replayedError{sentinel: ErrExecutionStopped}, ErrExecutionStopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errors.Unwrap(tt.err)
			if got != tt.want {
				t.Errorf("Unwrap = %v, want %v", got, tt.want)
			}
		})
	}
}

// typedSerdesCause is a named cause type for SerdesError chain matching.
type typedSerdesCause struct{ msg string }

func (e *typedSerdesCause) Error() string { return e.msg }

func TestSerdesErrorIsAsChain(t *testing.T) {
	sentinel := errors.New("bad payload")
	wrapped := &SerdesError{Operation: "op", Direction: "unmarshal", Err: fmt.Errorf("decode: %w", sentinel)}
	if !errors.Is(wrapped, sentinel) {
		t.Error("errors.Is should reach the underlying cause through Unwrap")
	}

	cause := &typedSerdesCause{msg: "typed cause"}
	typed := &SerdesError{Operation: "op", Direction: "marshal", Err: cause}
	var got *typedSerdesCause
	if !errors.As(typed, &got) {
		t.Fatal("errors.As should reach the underlying cause through Unwrap")
	}
	if got != cause {
		t.Errorf("errors.As extracted %v, want %v", got, cause)
	}
}

func TestCombinatorErrorMultiUnwrap(t *testing.T) {
	e1 := fmt.Errorf("one")
	e2 := fmt.Errorf("two")
	combo := &CombinatorError{Name: "any", Errors: []error{e1, e2}}
	if !errors.Is(combo, e1) {
		t.Error("CombinatorError should match first child via Is")
	}
	if !errors.Is(combo, e2) {
		t.Error("CombinatorError should match second child via Is")
	}
}

// wireErrorTypeCases lists every exported error type with the wire
// ErrorType it must produce. For the types shared with the other Durable
// Execution SDKs the name is the shared wire name; for the Go-only types
// it is the Go type name. Adding an exported error type requires adding a
// row here, so the wire contract is stated in one place.
var wireErrorTypeCases = []struct {
	name string
	err  error
	want string
}{
	{"StepError", &StepError{Name: "s", Attempts: 2, Err: fmt.Errorf("x")}, "StepError"},
	{"StepInterruptedError", &StepInterruptedError{Name: "s"}, "StepInterruptedError"},
	{"InvokeError", &InvokeError{Name: "i", Err: fmt.Errorf("x")}, "InvokeError"},
	{"CallbackError", &CallbackError{Name: "c", Err: fmt.Errorf("x")}, "CallbackError"},
	{"ChildContextError", &ChildContextError{Name: "cc", Err: fmt.Errorf("x")}, "ChildContextError"},
	{"WaitForConditionError", &WaitForConditionError{Name: "w", Err: fmt.Errorf("x")}, "WaitForConditionError"},
	{"RetryError", &RetryError{Name: "r", Attempts: 3, Err: fmt.Errorf("x")}, "RetryError"},
	{"CombinatorError", &CombinatorError{Name: "any", Errors: []error{fmt.Errorf("x")}}, "PromiseCombinatorError"},
	{"BatchError", &BatchError{Name: "b", Reason: CompletionFailureToleranceExceeded, Errors: []error{fmt.Errorf("x")}}, "BatchError"},
	{"OperationError", &OperationError{Name: "op", Err: fmt.Errorf("x")}, "OperationError"},
	{"SerdesError", &SerdesError{Operation: "op", Direction: "marshal", Err: fmt.Errorf("x")}, "SerdesError"},
	{"NonDeterministicReplayError", &NonDeterministicReplayError{Name: "n"}, "NonDeterministicReplayError"},
	{"ResultTooLargeError", &ResultTooLargeError{Name: "r"}, "ResultTooLargeError"},
	{"CheckpointError", &CheckpointError{Err: fmt.Errorf("x")}, "CheckpointError"},
}

// wireErrorTypeOnBothPaths returns the ErrorType produced by the checkpoint
// path (errorObject) and by the invocation-response path
// (errorObjectFromError).
func wireErrorTypeOnBothPaths(err error) (checkpoint, response string) {
	return *errorObject(err).ErrorType, errorObjectFromError(err, nil).ErrorType
}

func TestWireErrorTypeEveryExportedTypeOnBothPaths(t *testing.T) {
	for _, tt := range wireErrorTypeCases {
		t.Run(tt.name, func(t *testing.T) {
			cp, resp := wireErrorTypeOnBothPaths(tt.err)
			if cp != tt.want {
				t.Errorf("checkpoint ErrorType = %q, want %q", cp, tt.want)
			}
			if resp != tt.want {
				t.Errorf("response ErrorType = %q, want %q", resp, tt.want)
			}
		})
	}
}

func TestWireErrorTypeChildContextWrappingStep(t *testing.T) {
	// The outermost SDK type names the wire ErrorType on both paths.
	err := &ChildContextError{Name: "cc", Err: &StepError{Name: "s", Attempts: 1, Err: fmt.Errorf("x")}}
	cp, resp := wireErrorTypeOnBothPaths(err)
	if cp != "ChildContextError" || resp != "ChildContextError" {
		t.Errorf("ErrorType = (checkpoint %q, response %q), want ChildContextError on both", cp, resp)
	}
}

func TestWireErrorTypeCombinator(t *testing.T) {
	err := &CombinatorError{Name: "any", Errors: []error{fmt.Errorf("x")}}
	cp, resp := wireErrorTypeOnBothPaths(err)
	if cp != "PromiseCombinatorError" || resp != "PromiseCombinatorError" {
		t.Errorf("ErrorType = (checkpoint %q, response %q), want PromiseCombinatorError on both", cp, resp)
	}
}

type userDefinedError struct{ msg string }

func (e *userDefinedError) Error() string { return e.msg }

// userWrapError is a user-defined error that wraps a cause.
type userWrapError struct{ cause error }

func (e *userWrapError) Error() string { return "user wrap: " + e.cause.Error() }
func (e *userWrapError) Unwrap() error { return e.cause }

func TestWireErrorTypeUserDefinedOnBothPaths(t *testing.T) {
	cp, resp := wireErrorTypeOnBothPaths(&userDefinedError{"boom"})
	if cp != "userDefinedError" || resp != "userDefinedError" {
		t.Errorf("ErrorType = (checkpoint %q, response %q), want userDefinedError on both", cp, resp)
	}
}

func TestWireErrorTypeWrappedChains(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"fmt.Errorf around StepError", fmt.Errorf("w: %w", &StepError{Err: fmt.Errorf("x")}), "StepError"},
		{"user wrapper around StepError", &userWrapError{&StepError{Err: fmt.Errorf("x")}}, "StepError"},
		{"user wrapper around plain error", &userWrapError{fmt.Errorf("x")}, "userWrapError"},
		{"StepError around user type", &StepError{Err: &userDefinedError{"inner"}}, "StepError"},
		{"CallbackError around ChildContextError", &CallbackError{Err: &ChildContextError{Err: fmt.Errorf("x")}}, "CallbackError"},
		{"errors.Join with StepError second", errors.Join(fmt.Errorf("a"), &InvokeError{Err: fmt.Errorf("b")}), "InvokeError"},
		{"plain error", fmt.Errorf("plain"), "Error"},
		{"nil-cause wrapper", &ChildContextError{Name: "cc"}, "ChildContextError"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp, resp := wireErrorTypeOnBothPaths(tt.err)
			if cp != tt.want || resp != tt.want {
				t.Errorf("ErrorType = (checkpoint %q, response %q), want %q on both", cp, resp, tt.want)
			}
		})
	}
}

func TestErrorObjectFromErrorMessage(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"callback reports cause", &CallbackError{Name: "c", Err: fmt.Errorf("external")}, "external"},
		{"callback replayed cause", &CallbackError{Name: "c", Err: &replayedError{errType: "T", message: "m"}}, "m"},
		{"child context reports cause", &ChildContextError{Name: "cc", Err: fmt.Errorf("inner")}, "inner"},
		{"child context wrapping step reports step message",
			&ChildContextError{Name: "cc", Err: &StepError{Name: "s", Attempts: 1, Err: fmt.Errorf("x")}},
			(&StepError{Name: "s", Attempts: 1, Err: fmt.Errorf("x")}).Error()},
		{"child context reports recorded message over cause",
			&ChildContextError{Name: "cc", ErrorType: "StepError", Message: "recorded", Err: fmt.Errorf("x")}, "recorded"},
		{"step reports own message", &StepError{Name: "s", Attempts: 1, Err: fmt.Errorf("x")},
			(&StepError{Name: "s", Attempts: 1, Err: fmt.Errorf("x")}).Error()},
		{"plain error", fmt.Errorf("plain"), "plain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorObjectFromError(tt.err, nil).ErrorMessage; got != tt.want {
				t.Errorf("ErrorMessage = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSentinelErrorStrings(t *testing.T) {
	// Verify sentinel error messages are stable for documentation.
	tests := []struct {
		err  error
		want string
	}{
		{ErrCallbackTimedOut, "durable: callback timed out"},
		{ErrInvokeTimedOut, "durable: invoke timed out"},
		{ErrExecutionStopped, "durable: execution stopped"},
		{ErrExecutionCancelled, "durable: execution cancelled"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
}
