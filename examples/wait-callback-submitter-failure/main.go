// Command wait-callback-submitter-failure demonstrates WaitForCallback
// where the submitter function itself throws an error. The error
// propagates as a failure without retry (NoRetry strategy).
package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	_, err := durable.WaitForCallback[string](ctx, "failing-submitter-callback",
		func(_ durable.StepContext, _ string) error {
			return fmt.Errorf("submitter failed: service unavailable")
		},
		durable.WithSubmitterRetry(durable.NoRetry()),
	)
	if err != nil {
		var cbErr *durable.CallbackError
		if errors.As(err, &cbErr) {
			return Result{Success: false, Error: cbErr.Error()}, nil
		}
		return Result{Success: false, Error: err.Error()}, nil
	}
	return Result{Success: true}, nil
}

func main() { durable.Start(handler) }
