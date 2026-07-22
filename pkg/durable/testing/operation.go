package testing

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// callbackDriver is the minimal, generics-free interface an Operation
// needs from its owning runner to drive a callback to completion. Kept
// separate from LocalTestRunner[TEvent, TResult] itself because Operation
// must be usable without threading the runner's type parameters through
// every accessor — Go generics cannot express "some LocalTestRunner[T, U]
// for unknown T, U" any other way.
type callbackDriver interface {
	sendCallbackResult(callbackID string, result *string, errObj *types.ErrorObject) error
}

// Operation is a read-only handle onto a single entry in the operation
// history produced by a test run, and (for CALLBACK operations) a handle
// for driving that operation from the test itself.
//
// The accessor set mirrors the official cross-SDK Testing API Reference's
// Operation/TestOperation type as closely as Go's typing model allows
// (docs.aws.amazon.com/durable-execution/testing/api-reference/,
// "Operation" / "Common accessors" / "Step details" / etc.): GetName,
// GetType, GetStatus, GetStepDetails, and so on. Go has no exception
// hierarchy or generic deserialization-by-Class the way Java's
// getStepResult(Class<T>) does, so StepResult here is generic
// (StepResult[T](op)) rather than a method - see that function's doc.
type Operation struct {
	raw types.Operation

	// runner is set only for operations that support driving a callback
	// from the operation handle (see SendCallbackSuccess/Failure). nil for
	// operations obtained from a TestResult snapshot that isn't
	// callback-capable (never expected in practice, but avoids requiring
	// every accessor method to carry a runner reference).
	runner callbackDriver
}

// GetID returns the operation's unique ID within its execution.
func (o Operation) GetID() string { return o.raw.ID }

// GetParentID returns the ID of this operation's parent context
// operation, or "" for a top-level operation.
func (o Operation) GetParentID() string { return o.raw.ParentID }

// GetName returns the operation's user-assigned name.
func (o Operation) GetName() string { return o.raw.Name }

// GetType returns the operation's type (STEP, WAIT, CALLBACK,
// CHAINED_INVOKE, CONTEXT, or EXECUTION).
func (o Operation) GetType() types.OperationType { return o.raw.Type }

// GetStatus returns the operation's current status.
func (o Operation) GetStatus() types.OperationStatus { return o.raw.Status }

// GetStartTimestamp returns when the operation started, if recorded.
func (o Operation) GetStartTimestamp() *types.Time { return o.raw.StartTimestamp }

// GetEndTimestamp returns when the operation reached a terminal status, if
// recorded.
func (o Operation) GetEndTimestamp() *types.Time { return o.raw.EndTimestamp }

// GetStepDetails returns the operation's STEP-specific details, or nil if
// this is not a STEP operation.
func (o Operation) GetStepDetails() *types.StepDetails { return o.raw.StepDetails }

// GetWaitDetails returns the operation's WAIT-specific details, or nil if
// this is not a WAIT operation.
func (o Operation) GetWaitDetails() *types.WaitDetails { return o.raw.WaitDetails }

// GetCallbackDetails returns the operation's CALLBACK-specific details, or
// nil if this is not a CALLBACK operation.
func (o Operation) GetCallbackDetails() *types.CallbackDetails { return o.raw.CallbackDetails }

// GetChainedInvokeDetails returns the operation's CHAINED_INVOKE-specific
// details, or nil if this is not a CHAINED_INVOKE operation.
func (o Operation) GetChainedInvokeDetails() *types.ChainedInvokeDetails {
	return o.raw.ChainedInvokeDetails
}

// GetContextDetails returns the operation's CONTEXT-specific details, or
// nil if this is not a CONTEXT (child context) operation.
func (o Operation) GetContextDetails() *types.ContextDetails { return o.raw.ContextDetails }

// GetExecutionDetails returns the operation's EXECUTION-specific details,
// or nil if this is not the root EXECUTION operation.
func (o Operation) GetExecutionDetails() *types.ExecutionDetails { return o.raw.ExecutionDetails }

// GetError returns the structured error recorded for this operation, if
// it failed, regardless of operation type. Returns nil if the operation
// did not fail or has no recorded error.
func (o Operation) GetError() *types.ErrorObject {
	switch o.raw.Type {
	case types.OperationTypeStep:
		if o.raw.StepDetails != nil {
			return o.raw.StepDetails.Error
		}
	case types.OperationTypeCallback:
		if o.raw.CallbackDetails != nil {
			return o.raw.CallbackDetails.Error
		}
	case types.OperationTypeChainedInvoke:
		if o.raw.ChainedInvokeDetails != nil {
			return o.raw.ChainedInvokeDetails.Error
		}
	case types.OperationTypeContext:
		if o.raw.ContextDetails != nil {
			return o.raw.ContextDetails.Error
		}
	}
	return nil
}

// StepResult deserializes a STEP operation's checkpointed result into T.
// This is Go's equivalent of Java's getStepResult(Class<T>) / TypeScript's
// getStepDetails()?.result — a free function rather than a method because
// Go methods cannot be parameterized independently of their receiver
// type.
func StepResult[T any](op Operation) (T, error) {
	var zero T
	if op.raw.StepDetails == nil || op.raw.StepDetails.Result == nil {
		return zero, fmt.Errorf("testing.StepResult: operation %q has no recorded step result", op.raw.Name)
	}
	if err := json.Unmarshal([]byte(*op.raw.StepDetails.Result), &zero); err != nil {
		return zero, fmt.Errorf("testing.StepResult: unmarshaling operation %q's result into %T: %w", op.raw.Name, zero, err)
	}
	return zero, nil
}

// ContextResult deserializes a CONTEXT (RunInChildContext/Map/Parallel)
// operation's checkpointed result into T, matching TypeScript's
// getContextDetails()?.result / Python's ctx.result.
func ContextResult[T any](op Operation) (T, error) {
	var zero T
	if op.raw.ContextDetails == nil || op.raw.ContextDetails.Result == nil {
		return zero, fmt.Errorf("testing.ContextResult: operation %q has no recorded context result", op.raw.Name)
	}
	if err := json.Unmarshal([]byte(*op.raw.ContextDetails.Result), &zero); err != nil {
		return zero, fmt.Errorf("testing.ContextResult: unmarshaling operation %q's result into %T: %w", op.raw.Name, zero, err)
	}
	return zero, nil
}

// SendCallbackSuccess completes a CALLBACK operation with a successful
// result, mirroring the official runner API's
// sendCallbackSuccess/completeCallback (see the Testing API Reference's
// "Drive callbacks" / "Drive a callback from an operation" sections).
// result is marshaled as JSON. Only valid for CALLBACK-type operations
// obtained from a LocalTestRunner (not from a TestResult snapshot taken
// after Run() returned an already-terminal execution).
func (o Operation) SendCallbackSuccess(result any) error {
	if o.runner == nil {
		return fmt.Errorf("testing.Operation.SendCallbackSuccess: operation %q was not obtained from an active LocalTestRunner", o.raw.Name)
	}
	if o.raw.Type != types.OperationTypeCallback {
		return fmt.Errorf("testing.Operation.SendCallbackSuccess: operation %q is not a CALLBACK operation (type=%s)", o.raw.Name, o.raw.Type)
	}
	if o.raw.CallbackDetails == nil || o.raw.CallbackDetails.CallbackID == "" {
		return fmt.Errorf("testing.Operation.SendCallbackSuccess: operation %q has no callback ID yet (has the handler reached CreateCallback/WaitForCallback?)", o.raw.Name)
	}
	b, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("testing.Operation.SendCallbackSuccess: marshaling result: %w", err)
	}
	payload := string(b)
	return o.runner.sendCallbackResult(o.raw.CallbackDetails.CallbackID, &payload, nil)
}

// SendCallbackFailure completes a CALLBACK operation with a failure,
// mirroring the official runner API's sendCallbackFailure/failCallback.
func (o Operation) SendCallbackFailure(errObj types.ErrorObject) error {
	if o.runner == nil {
		return fmt.Errorf("testing.Operation.SendCallbackFailure: operation %q was not obtained from an active LocalTestRunner", o.raw.Name)
	}
	if o.raw.Type != types.OperationTypeCallback {
		return fmt.Errorf("testing.Operation.SendCallbackFailure: operation %q is not a CALLBACK operation (type=%s)", o.raw.Name, o.raw.Type)
	}
	if o.raw.CallbackDetails == nil || o.raw.CallbackDetails.CallbackID == "" {
		return fmt.Errorf("testing.Operation.SendCallbackFailure: operation %q has no callback ID yet (has the handler reached CreateCallback/WaitForCallback?)", o.raw.Name)
	}
	return o.runner.sendCallbackResult(o.raw.CallbackDetails.CallbackID, nil, &errObj)
}
