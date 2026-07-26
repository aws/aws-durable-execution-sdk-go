// Command wait-callback-error-instance-timeout verifies that
// CallbackError from a timeout is correctly typed through replay.
// The error chain propagates through WaitForCallback's child context.
package main

import (
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	IsCallbackError  bool   `json:"isCallbackError"`
	ContainsTimedOut bool   `json:"containsTimedOut"`
	ErrorMessage     string `json:"errorMessage,omitempty"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	var cbErr error

	_, err := durable.WaitForCallback[string](ctx, "timeout-test",
		func(_ durable.StepContext, _ string) error {
			return nil // Submitter succeeds but no completion arrives.
		},
		durable.WithCallbackTimeout(3*time.Second),
	)
	if err != nil {
		cbErr = err
	}

	// Wait to force a replay cycle.
	if waitErr := durable.Wait(ctx, "post-timeout-wait", 1*time.Second); waitErr != nil {
		return Result{}, waitErr
	}

	// Verify error type in a step (ensures replay preserves type).
	result, err := durable.Step[Result](ctx, "check-error-type",
		func(_ durable.StepContext) (Result, error) {
			var callbackErr *durable.CallbackError
			isCbErr := errors.As(cbErr, &callbackErr)
			msg := ""
			if cbErr != nil {
				msg = cbErr.Error()
			}
			// WaitForCallback wraps the timeout through a child context,
			// so we check the message contains the timeout sentinel text.
			containsTimeout := strings.Contains(msg, "callback timed out")
			return Result{
				IsCallbackError:  isCbErr,
				ContainsTimedOut: containsTimeout,
				ErrorMessage:     msg,
			}, nil
		})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func main() { durable.Start(handler) }
