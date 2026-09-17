// Command wait-callback-error-instance-submitter verifies that a failing
// WaitForCallback submitter is returned as a CallbackSubmitterError that
// matches CallbackError and carries the submitter's error type and message.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	IsCallbackError  bool   `json:"isCallbackError"`
	IsSubmitterError bool   `json:"isSubmitterError"`
	ErrorType        string `json:"errorType,omitempty"`
	ErrorMessage     string `json:"errorMessage,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	var cbErr error

	_, err := durable.WaitForCallback[string](ctx, "submitter-test",
		func(_ durable.StepContext, _ string) error {
			return fmt.Errorf("submitter failed")
		},
		durable.WithSubmitterRetry(durable.NoRetry()),
	)
	if err != nil {
		cbErr = err
	}

	// Verify error type in a step.
	result, err := durable.Step[Result](ctx, "check-error-type",
		func(_ durable.StepContext) (Result, error) {
			var callbackErr *durable.CallbackError
			var submitterErr *durable.CallbackSubmitterError
			result := Result{
				IsCallbackError:  errors.As(cbErr, &callbackErr),
				IsSubmitterError: errors.As(cbErr, &submitterErr),
			}
			if callbackErr != nil {
				result.ErrorType = callbackErr.ErrorType
				result.ErrorMessage = callbackErr.Message
			}
			return result, nil
		})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func main() { durable.Start(handler) }
