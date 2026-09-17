// Command wait-callback-error-instance-timeout verifies that a
// WaitForCallback timeout is returned as a CallbackTimeoutError that
// matches CallbackError and ErrCallbackTimedOut, and that the same typed
// error is observed after a replay.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	IsCallbackError  bool   `json:"isCallbackError"`
	IsTimeoutError   bool   `json:"isTimeoutError"`
	ContainsTimedOut bool   `json:"containsTimedOut"`
	Heartbeat        bool   `json:"heartbeat"`
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
			var timeoutErr *durable.CallbackTimeoutError
			msg := ""
			if cbErr != nil {
				msg = cbErr.Error()
			}
			result := Result{
				IsCallbackError:  errors.As(cbErr, &callbackErr),
				IsTimeoutError:   errors.As(cbErr, &timeoutErr),
				ContainsTimedOut: errors.Is(cbErr, durable.ErrCallbackTimedOut),
				ErrorMessage:     msg,
			}
			if timeoutErr != nil {
				result.Heartbeat = timeoutErr.Heartbeat
			}
			return result, nil
		})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func main() { durable.Start(handler) }
