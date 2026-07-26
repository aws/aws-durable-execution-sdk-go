// Command error-handling-taxonomy demonstrates the SDK's typed error
// hierarchy. Each durable operation that can fail produces a specific error
// type (StepError, InvokeError, CallbackError) that is matchable with
// errors.As and participates in the OperationError super-type.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Output records the taxonomy of each error the handler encountered.
type Output struct {
	StepErrorInfo     ErrorInfo `json:"stepErrorInfo"`
	InvokeErrorInfo   ErrorInfo `json:"invokeErrorInfo"`
	CallbackErrorInfo ErrorInfo `json:"callbackErrorInfo"`
}

// ErrorInfo captures the key fields extracted from a typed error via errors.As.
type ErrorInfo struct {
	Matched       bool   `json:"matched"`
	TypeName      string `json:"typeName"`
	OperationName string `json:"operationName"`
	Attempts      int    `json:"attempts,omitempty"`
	IsOpError     bool   `json:"isOpError"`
	OpErrorName   string `json:"opErrorName,omitempty"`
}

func handler(ctx durable.Context, _ any) (Output, error) {
	var out Output

	// 1. Provoke a StepError: a step that always fails with no retry.
	_, err := durable.Step(ctx, "failing-step",
		func(_ durable.StepContext) (any, error) {
			return nil, fmt.Errorf("deliberate step failure")
		},
		durable.WithRetry(durable.NoRetry()),
	)
	if err != nil {
		var stepErr *durable.StepError
		if errors.As(err, &stepErr) {
			var opErr *durable.OperationError
			isOp := errors.As(err, &opErr)
			out.StepErrorInfo = ErrorInfo{
				Matched:       true,
				TypeName:      "StepError",
				OperationName: stepErr.Name,
				Attempts:      stepErr.Attempts,
				IsOpError:     isOp,
			}
			if isOp {
				out.StepErrorInfo.OpErrorName = opErr.Name
			}
		}
	}

	// 2. Provoke an InvokeError: invoke a function that the test will fail.
	_, err = durable.Invoke[any](ctx, "failing-invoke",
		"arn:aws:lambda:us-east-1:123456789012:function:nonexistent",
		struct{}{},
	)
	if err != nil {
		var invokeErr *durable.InvokeError
		if errors.As(err, &invokeErr) {
			var opErr *durable.OperationError
			isOp := errors.As(err, &opErr)
			out.InvokeErrorInfo = ErrorInfo{
				Matched:       true,
				TypeName:      "InvokeError",
				OperationName: invokeErr.Name,
				IsOpError:     isOp,
			}
			if isOp {
				out.InvokeErrorInfo.OpErrorName = opErr.Name
			}
		}
	}

	// 3. Provoke a CallbackError: create a callback the test will fail.
	cb, err := durable.CreateCallback[string](ctx, "failing-callback")
	if err != nil {
		return Output{}, err
	}
	_, err = cb.Result()
	if err != nil {
		var cbErr *durable.CallbackError
		if errors.As(err, &cbErr) {
			var opErr *durable.OperationError
			isOp := errors.As(err, &opErr)
			out.CallbackErrorInfo = ErrorInfo{
				Matched:       true,
				TypeName:      "CallbackError",
				OperationName: cbErr.Name,
				IsOpError:     isOp,
			}
			if isOp {
				out.CallbackErrorInfo.OpErrorName = opErr.Name
			}
		}
	}

	return out, nil
}

func main() { durable.Start(handler) }
