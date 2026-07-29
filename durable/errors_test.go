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

func TestErrorObjectFromError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantType string
	}{
		{"StepError", &StepError{Err: fmt.Errorf("x")}, "StepError"},
		{"InvokeError", &InvokeError{Err: fmt.Errorf("x")}, "InvokeError"},
		{"CallbackError", &CallbackError{Err: fmt.Errorf("x")}, "CallbackError"},
		{"ChildContextError", &ChildContextError{Err: fmt.Errorf("x")}, "ChildContextError"},
		{"WaitForConditionError", &WaitForConditionError{Err: fmt.Errorf("x")}, "WaitForConditionError"},
		{"CombinatorError", &CombinatorError{Errors: []error{fmt.Errorf("x")}}, "PromiseCombinatorError"},
		{"plain error", fmt.Errorf("plain"), "Error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			we := errorObjectFromError(tt.err)
			if we.ErrorType != tt.wantType {
				t.Errorf("ErrorType = %q, want %q", we.ErrorType, tt.wantType)
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
