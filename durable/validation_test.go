package durable

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// --- Item 1: Replay consistency validation ---

func TestValidateReplayConsistency(t *testing.T) {
	tests := []struct {
		name            string
		op              *operation
		expectedType    string
		expectedSubType string
		expectedName    string
		wantErr         bool
		wantErrType     string // if wantErr, check the actual type in the error
	}{
		{
			name:            "nil operation (first execution)",
			op:              nil,
			expectedType:    "STEP",
			expectedSubType: "Step",
			expectedName:    "my-step",
			wantErr:         false,
		},
		{
			name:            "empty opType (backend omitted Type)",
			op:              &operation{id: "abc123", opType: ""},
			expectedType:    "STEP",
			expectedSubType: "Step",
			expectedName:    "my-step",
			wantErr:         false,
		},
		{
			name:            "matching type and subtype",
			op:              &operation{id: "abc123", opType: "STEP", subType: "Step", name: "my-step"},
			expectedType:    "STEP",
			expectedSubType: "Step",
			expectedName:    "my-step",
			wantErr:         false,
		},
		{
			name:            "matching type with empty expected subtype",
			op:              &operation{id: "abc123", opType: "STEP", subType: "WaitForCondition", name: "my-step"},
			expectedType:    "STEP",
			expectedSubType: "",
			expectedName:    "my-step",
			wantErr:         false,
		},
		{
			name:            "type mismatch (STEP vs WAIT)",
			op:              &operation{id: "abc123", opType: "STEP", subType: "Step", name: "my-step"},
			expectedType:    "WAIT",
			expectedSubType: "Wait",
			expectedName:    "my-step",
			wantErr:         true,
			wantErrType:     "STEP",
		},
		{
			name:            "subtype mismatch (Step vs WaitForCondition)",
			op:              &operation{id: "abc123", opType: "STEP", subType: "Step", name: "my-step"},
			expectedType:    "STEP",
			expectedSubType: "WaitForCondition",
			expectedName:    "my-step",
			wantErr:         true,
			wantErrType:     "STEP",
		},
		{
			name:            "name mismatch",
			op:              &operation{id: "abc123", opType: "STEP", subType: "Step", name: "old-name"},
			expectedType:    "STEP",
			expectedSubType: "Step",
			expectedName:    "new-name",
			wantErr:         true,
			wantErrType:     "STEP",
		},
		{
			name:            "invoke type mismatch to callback",
			op:              &operation{id: "abc123", opType: "CHAINED_INVOKE", subType: "ChainedInvoke", name: "call-fn"},
			expectedType:    "CALLBACK",
			expectedSubType: "Callback",
			expectedName:    "call-fn",
			wantErr:         true,
			wantErrType:     "CHAINED_INVOKE",
		},
		{
			name:            "context type with different subtype (Map vs Parallel)",
			op:              &operation{id: "abc123", opType: "CONTEXT", subType: "Map", name: "batch"},
			expectedType:    "CONTEXT",
			expectedSubType: "Parallel",
			expectedName:    "batch",
			wantErr:         true,
			wantErrType:     "CONTEXT",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateReplayConsistency(tc.op, tc.expectedType, tc.expectedSubType, tc.expectedName)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				var ndErr *NonDeterministicExecutionError
				if !errors.As(err, &ndErr) {
					t.Fatalf("expected *NonDeterministicExecutionError, got %T: %v", err, err)
				}
				if ndErr.RecordedType != tc.wantErrType {
					t.Errorf("RecordedType = %q, want %q", ndErr.RecordedType, tc.wantErrType)
				}
				if ndErr.CurrentType != tc.expectedType {
					t.Errorf("CurrentType = %q, want %q", ndErr.CurrentType, tc.expectedType)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNonDeterministicExecutionError_ErrorMessage(t *testing.T) {
	err := &NonDeterministicExecutionError{
		Name:            "my-wait",
		StepID:          "1",
		CurrentType:     "WAIT",
		CurrentSubType:  "Wait",
		CurrentName:     "my-wait",
		RecordedType:    "STEP",
		RecordedSubType: "Step",
		RecordedName:    "my-wait",
	}
	want := `durable: non-deterministic replay at step "1": ` +
		`the checkpoint recorded operation (type STEP, subtype "Step", name "my-wait"), ` +
		`but the handler now runs (type WAIT, subtype "Wait", name "my-wait"). ` +
		`The handler code changed between deployments.`
	if got := err.Error(); got != want {
		t.Errorf("Error() =\n%s\nwant\n%s", got, want)
	}
	if strings.Contains(err.Error(), "\u2014") {
		t.Errorf("Error() contains an em dash: %s", err.Error())
	}
}

// --- Item 3: OperationError base matching ---

func TestOperationErrorBase_AllTypedErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "StepError",
			err:  &StepError{Name: "s1", Attempts: 3, Err: errors.New("fail")},
		},
		{
			name: "StepInterruptedError",
			err:  &StepInterruptedError{Name: "s2"},
		},
		{
			name: "InvokeError",
			err:  &InvokeError{Name: "inv", FunctionID: "fn", Err: errors.New("timeout")},
		},
		{
			name: "CallbackError",
			err:  &CallbackError{Name: "cb", CallbackID: "id1", Err: errors.New("rejected")},
		},
		{
			name: "ChildContextError",
			err:  &ChildContextError{Name: "child", Err: errors.New("boom")},
		},
		{
			name: "WaitForConditionError",
			err:  &WaitForConditionError{Name: "wfc", Attempts: 5, Err: errors.New("timeout")},
		},
		{
			name: "CombinatorError",
			err:  &CombinatorError{Name: "all", Errors: []error{errors.New("a"), errors.New("b")}},
		},
		{
			name: "NonDeterministicExecutionError",
			err: &NonDeterministicExecutionError{
				Name:         "nd",
				StepID:       "1",
				CurrentType:  "STEP",
				RecordedType: "WAIT",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Direct matching.
			var opErr *OperationError
			if !errors.As(tc.err, &opErr) {
				t.Fatalf("errors.As(err, &OperationError) failed for %T", tc.err)
			}

			// Through a wrapping layer.
			wrapped := fmt.Errorf("outer: %w", tc.err)
			var opErr2 *OperationError
			if !errors.As(wrapped, &opErr2) {
				t.Fatalf("errors.As(wrapped, &OperationError) failed for %T", tc.err)
			}
		})
	}
}

func TestOperationError_ExistingUnwrapPreserved(t *testing.T) {
	// Verify that existing Unwrap chains still work: errors.Is can find
	// sentinels wrapped inside typed errors.
	sentinel := errors.New("inner sentinel")
	stepErr := &StepError{Name: "s", Attempts: 1, Err: sentinel}
	if !errors.Is(stepErr, sentinel) {
		t.Fatal("errors.Is should find sentinel through StepError.Unwrap()")
	}

	invokeErr := &InvokeError{Name: "i", FunctionID: "f", Err: ErrInvokeTimedOut}
	if !errors.Is(invokeErr, ErrInvokeTimedOut) {
		t.Fatal("errors.Is should find ErrInvokeTimedOut through InvokeError.Unwrap()")
	}

	callbackErr := &CallbackError{Name: "c", CallbackID: "id", Err: ErrCallbackTimedOut}
	if !errors.Is(callbackErr, ErrCallbackTimedOut) {
		t.Fatal("errors.Is should find ErrCallbackTimedOut through CallbackError.Unwrap()")
	}
}

func TestOperationError_DoesNotMatchNonOperationError(t *testing.T) {
	// A plain error should NOT match *OperationError.
	plainErr := errors.New("not an operation error")
	var opErr *OperationError
	if errors.As(plainErr, &opErr) {
		t.Fatal("plain error should not match *OperationError")
	}
}

func TestNonDeterministicExecutionError_MatchesOperationError(t *testing.T) {
	err := &NonDeterministicExecutionError{
		Name:         "step-a",
		StepID:       "1",
		CurrentType:  "STEP",
		RecordedType: "WAIT",
	}
	// Should match via As.
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatal("NonDeterministicExecutionError should match OperationError via As")
	}
	if opErr.Name != "step-a" {
		t.Errorf("OperationError.Name = %q, want %q", opErr.Name, "step-a")
	}
}
