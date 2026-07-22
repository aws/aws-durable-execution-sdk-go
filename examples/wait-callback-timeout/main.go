// Command wait-callback-timeout demonstrates a WaitForCallback that
// times out. The submitter succeeds (dispatching the callback ID) but no
// external system completes the callback within the timeout window.
package main

import (
	"errors"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type Result struct {
	TimedOut bool   `json:"timedOut"`
	Error    string `json:"error"`
}

func handler(ctx durable.Context, _ any) (Result, error) {
	_, err := durable.WaitForCallback[string](ctx, "timeout-callback",
		func(sctx durable.StepContext, callbackID string) error {
			sctx.Logger().Info("Submitter dispatched, callback will timeout",
				"callbackId", callbackID)
			// Submitter succeeds but no one completes the callback.
			return nil
		},
		durable.WithCallbackTimeout(3*time.Second),
	)
	if err != nil {
		var cbErr *durable.CallbackError
		if errors.As(err, &cbErr) && errors.Is(cbErr.Err, durable.ErrCallbackTimedOut) {
			return Result{TimedOut: true, Error: cbErr.Error()}, nil
		}
		return Result{}, err
	}
	return Result{TimedOut: false}, nil
}

func main() { durable.Start(handler) }
