// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// ExecutionStatus represents the outcome of a durable execution invocation.
type ExecutionStatus string

// Execution status values.
const (
	Succeeded ExecutionStatus = "SUCCEEDED"
	Failed    ExecutionStatus = "FAILED"
	Pending   ExecutionStatus = "PENDING"
)

// TestResult holds the outcome of a single [LocalRunner.Run] invocation.
// It captures the execution status, raw result payload, error details, and
// the operations that were checkpointed during the invocation.
type TestResult struct {
	// Status is the invocation outcome.
	Status ExecutionStatus

	// RawResult is the JSON-encoded handler result. Present only when
	// Status is [Succeeded]. Use [ResultAs] to deserialize.
	RawResult string

	// Error holds the error details when Status is [Failed].
	Error *TestError

	// Operations is the ordered list of operations checkpointed during
	// this invocation, excluding the execution operation itself.
	Operations []TestOperation

	// CapReached is true if [LocalRunner.RunUntilComplete] exhausted its
	// invocation cap without reaching a terminal status.
	CapReached bool
}

// Operation returns the first operation with the given name, or nil if not
// found.
func (r *TestResult) Operation(name string) *TestOperation {
	for i := range r.Operations {
		if r.Operations[i].Name == name {
			return &r.Operations[i]
		}
	}
	return nil
}

// OperationByIndex returns the operation at the given zero-based index, or
// nil if the index is out of bounds.
func (r *TestResult) OperationByIndex(index int) *TestOperation {
	if index < 0 || index >= len(r.Operations) {
		return nil
	}
	return &r.Operations[index]
}

// OperationByID returns the operation with the given wire ID (hashed), or
// nil if not found.
func (r *TestResult) OperationByID(id string) *TestOperation {
	for i := range r.Operations {
		if r.Operations[i].ID == id {
			return &r.Operations[i]
		}
	}
	return nil
}

// OperationsByType returns all operations of the given type string (e.g.
// "STEP", "WAIT", "CALLBACK", "CHAINED_INVOKE", "CONTEXT").
func (r *TestResult) OperationsByType(opType string) []TestOperation {
	var result []TestOperation
	for _, op := range r.Operations {
		if op.Type == opType {
			result = append(result, op)
		}
	}
	return result
}

// TestError carries the error information from a FAILED invocation.
type TestError struct {
	// Type is the error's type name (e.g. "StepError", "InvokeError").
	Type string

	// Message is the human-readable error message.
	Message string
}

// TestOperation represents a single checkpointed durable operation.
type TestOperation struct {
	// ID is the wire operation ID (hashed).
	ID string

	// Name is the caller-supplied operation name, or empty for unnamed
	// operations.
	Name string

	// Status is the operation's current status.
	Status string

	// Type is the operation type (e.g. "STEP", "WAIT", "CALLBACK").
	Type string

	// SubType further qualifies the operation type (e.g. "Step",
	// "WaitForCondition", "RunInChildContext").
	SubType string

	// ParentID is the parent operation's wire ID, or empty for top-level
	// operations.
	ParentID string

	// StepDetails holds step-specific details, if the operation is a step.
	StepDetails *TestStepDetails

	// CallbackDetails holds callback-specific details, if the operation
	// is a callback.
	CallbackDetails *TestCallbackDetails

	// InvokeDetails holds chained-invoke-specific details, if the
	// operation is a chained invoke.
	InvokeDetails *TestInvokeDetails

	// ContextDetails holds child-context-specific details, if the
	// operation is a context.
	ContextDetails *TestContextDetails

	// WaitDetails holds wait-specific details (currently empty; reserved
	// for future expansion).
	WaitDetails *TestWaitDetails
}

// IsTerminal reports whether the operation has reached a terminal status
// (SUCCEEDED, FAILED, CANCELLED, TIMED_OUT, STOPPED).
func (o *TestOperation) IsTerminal() bool {
	switch o.Status {
	case "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "STOPPED":
		return true
	}
	return false
}

// ResultAs deserializes the step result payload into the target type. It
// returns an error if the operation is not a step, has no result, or
// deserialization fails.
func OperationResultAs[O any](op *TestOperation) (O, error) {
	var zero O
	if op.StepDetails == nil || op.StepDetails.Result == "" {
		return zero, fmt.Errorf("durabletest: operation %q has no step result", op.Name)
	}
	var out O
	if err := json.Unmarshal([]byte(op.StepDetails.Result), &out); err != nil {
		return zero, fmt.Errorf("durabletest: deserialize operation %q step result: %w", op.Name, err)
	}
	return out, nil
}

// TestStepDetails carries step-specific checkpoint state.
type TestStepDetails struct {
	// Attempt is the number of attempts recorded.
	Attempt int32

	// Result is the serialized step result (present on SUCCEEDED).
	Result string

	// ErrorType is the recorded error type (present on FAILED/PENDING).
	ErrorType string

	// ErrorMessage is the recorded error message.
	ErrorMessage string
}

// TestCallbackDetails carries callback-specific checkpoint state.
type TestCallbackDetails struct {
	// CallbackID is the identifier for external callback submission.
	CallbackID string

	// Result is the serialized callback result (present on SUCCEEDED).
	Result string

	// ErrorType is the recorded error type (present on FAILED).
	ErrorType string

	// ErrorMessage is the recorded error message (present on FAILED).
	ErrorMessage string
}

// TestInvokeDetails carries chained-invoke-specific checkpoint state.
type TestInvokeDetails struct {
	// Result is the serialized invoke result (present on SUCCEEDED).
	Result string

	// ErrorType is the recorded error type (present on FAILED).
	ErrorType string

	// ErrorMessage is the recorded error message (present on FAILED).
	ErrorMessage string

	// ErrorData is additional error data (present on some failures).
	ErrorData string
}

// TestContextDetails carries child-context-specific checkpoint state.
type TestContextDetails struct {
	// Result is the serialized context result (present on SUCCEEDED).
	Result string

	// ReplayChildren indicates the context uses replay-children mode for
	// large payloads.
	ReplayChildren bool

	// ErrorType is the recorded error type (present on FAILED).
	ErrorType string

	// ErrorMessage is the recorded error message (present on FAILED).
	ErrorMessage string
}

// TestWaitDetails carries wait-specific checkpoint state. Currently empty;
// reserved for future expansion.
type TestWaitDetails struct{}

// ResultAs deserializes the raw result of a [Succeeded] [TestResult] into
// the target type O. It returns an error if the result is not present or
// deserialization fails.
//
// ResultAs is a package-level generic function because Go methods cannot
// have type parameters.
func ResultAs[O any](r *TestResult) (O, error) {
	var zero O
	if r.Status != Succeeded {
		return zero, fmt.Errorf("durabletest: cannot deserialize result from %s execution", r.Status)
	}
	if r.RawResult == "" {
		return zero, nil
	}
	var result O
	if err := json.Unmarshal([]byte(r.RawResult), &result); err != nil {
		return zero, fmt.Errorf("durabletest: deserialize result: %w", err)
	}
	return result, nil
}

// testResultFromResponse parses an invocation response and builds a
// TestResult with the operation snapshot.
func testResultFromResponse(response []byte, ops []durable.Operation) (*TestResult, error) {
	var resp struct {
		Status string  `json:"Status"`
		Result *string `json:"Result,omitempty"`
		Error  *struct {
			ErrorType    string `json:"ErrorType,omitempty"`
			ErrorMessage string `json:"ErrorMessage,omitempty"`
		} `json:"Error,omitempty"`
	}
	if err := json.Unmarshal(response, &resp); err != nil {
		return nil, fmt.Errorf("durabletest: parse invocation response: %w", err)
	}

	tr := &TestResult{
		Status:     ExecutionStatus(resp.Status),
		Operations: toTestOperations(ops),
	}
	if resp.Result != nil {
		tr.RawResult = *resp.Result
	}
	if resp.Error != nil {
		tr.Error = &TestError{
			Type:    resp.Error.ErrorType,
			Message: resp.Error.ErrorMessage,
		}
	}
	return tr, nil
}

// toTestOperations converts SDK operation types to TestOperations.
func toTestOperations(ops []durable.Operation) []TestOperation {
	result := make([]TestOperation, 0, len(ops))
	for _, op := range ops {
		to := TestOperation{
			ID:       ptrStr(op.Id),
			Name:     ptrStr(op.Name),
			Status:   string(op.Status),
			Type:     string(op.Type),
			SubType:  ptrStr(op.SubType),
			ParentID: ptrStr(op.ParentId),
		}
		if sd := op.StepDetails; sd != nil {
			to.StepDetails = &TestStepDetails{
				Attempt: sd.Attempt,
				Result:  ptrStr(sd.Result),
			}
			if sd.Error != nil {
				to.StepDetails.ErrorType = ptrStr(sd.Error.ErrorType)
				to.StepDetails.ErrorMessage = ptrStr(sd.Error.ErrorMessage)
			}
		}
		if cd := op.CallbackDetails; cd != nil {
			to.CallbackDetails = &TestCallbackDetails{
				CallbackID: ptrStr(cd.CallbackId),
				Result:     ptrStr(cd.Result),
			}
			if cd.Error != nil {
				to.CallbackDetails.ErrorType = ptrStr(cd.Error.ErrorType)
				to.CallbackDetails.ErrorMessage = ptrStr(cd.Error.ErrorMessage)
			}
		}
		if id := op.ChainedInvokeDetails; id != nil {
			to.InvokeDetails = &TestInvokeDetails{
				Result: ptrStr(id.Result),
			}
			if id.Error != nil {
				to.InvokeDetails.ErrorType = ptrStr(id.Error.ErrorType)
				to.InvokeDetails.ErrorMessage = ptrStr(id.Error.ErrorMessage)
				to.InvokeDetails.ErrorData = ptrStr(id.Error.ErrorData)
			}
		}
		if cd := op.ContextDetails; cd != nil {
			to.ContextDetails = &TestContextDetails{
				Result: ptrStr(cd.Result),
			}
			if cd.ReplayChildren != nil && *cd.ReplayChildren {
				to.ContextDetails.ReplayChildren = true
			}
			if cd.Error != nil {
				to.ContextDetails.ErrorType = ptrStr(cd.Error.ErrorType)
				to.ContextDetails.ErrorMessage = ptrStr(cd.Error.ErrorMessage)
			}
		}
		if op.WaitDetails != nil {
			to.WaitDetails = &TestWaitDetails{}
		}
		result = append(result, to)
	}
	return result
}
