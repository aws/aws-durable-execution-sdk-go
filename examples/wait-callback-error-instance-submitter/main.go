// Command wait-callback-error-instance-submitter verifies that a
// submitter failure error is correctly typed through replay cycles.
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	IsStepError  bool   `json:"isStepError"`
	ErrorMessage string `json:"errorMessage,omitempty"`
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
			var stepErr *durable.StepError
			isStepErr := errors.As(cbErr, &stepErr)
			msg := ""
			if cbErr != nil {
				msg = cbErr.Error()
			}
			return Result{IsStepError: isStepErr, ErrorMessage: msg}, nil
		})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func main() { durable.Start(handler) }
